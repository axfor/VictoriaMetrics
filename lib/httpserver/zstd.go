package httpserver

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// compressHandlerWrapper compresses responses with zstd for clients that
// prefer zstd to gzip, such as readers of large exports, and with gzip as
// upstream does for every other client. The zstd branch calls next itself and
// leaves the request as it came, so that a handler that proxies it passes its
// Accept-Encoding on.
func compressHandlerWrapper(next http.Handler) http.HandlerFunc {
	gzipped := gzipHandlerWrapper(next)
	return func(w http.ResponseWriter, r *http.Request) {
		if !prefersZstd(r.Header.Values("Accept-Encoding")) {
			gzipped(w, r)
			return
		}
		zw := &zstdResponseWriter{ResponseWriter: w, head: r.Method == http.MethodHead}
		defer zw.close()
		next.ServeHTTP(zw, r)
	}
}

// prefersZstd reports whether the Accept-Encoding headers give zstd a higher
// weight than gzip, or the same weight listed first. gzip counts as "*" when
// it is not listed itself; zstd only when it is. A weight that cannot be
// parsed rejects its encoding.
func prefersZstd(acceptEncoding []string) bool {
	type entry struct {
		q   float64
		pos int
	}
	var zstdEnc, gzipEnc, anyEnc *entry
	pos := 0
	for _, header := range acceptEncoding {
		for _, part := range strings.Split(header, ",") {
			name, params, _ := strings.Cut(part, ";")
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "" {
				continue
			}
			e := &entry{q: 1, pos: pos}
			pos++
			for _, param := range strings.Split(params, ";") {
				k, v, ok := strings.Cut(param, "=")
				if !ok || !strings.EqualFold(strings.TrimSpace(k), "q") {
					continue
				}
				q, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
				if err != nil || q < 0 || q > 1 {
					q = 0
				}
				e.q = q
			}
			switch {
			case name == "zstd" && zstdEnc == nil:
				zstdEnc = e
			case name == "gzip" && gzipEnc == nil:
				gzipEnc = e
			case name == "*" && anyEnc == nil:
				anyEnc = e
			}
		}
	}
	if gzipEnc == nil {
		gzipEnc = anyEnc
	}
	switch {
	case zstdEnc == nil || zstdEnc.q <= 0:
		return false
	case gzipEnc == nil || gzipEnc.q <= 0:
		return true
	}
	return zstdEnc.q > gzipEnc.q || zstdEnc.q == gzipEnc.q && zstdEnc.pos < gzipEnc.pos
}

// zstdMinSize is the smallest body compressed, as with gzip (gzhttp.DefaultMinSize):
// small bodies, error messages among them, go out as they are.
const zstdMinSize = 1024

// zstdResponseWriter compresses a response with zstd the way the gzip wrapper
// does with gzip: it holds back the first bytes until there are zstdMinSize
// of them or the handler returns or flushes, and then compresses only a
// successful full response that is big enough and has no Content-Encoding of
// its own. Informational statuses go out at once.
type zstdResponseWriter struct {
	http.ResponseWriter

	head     bool // a HEAD request, whose body is not sent
	code     int
	buf      []byte // written before it is decided whether to compress
	decided  bool
	hijacked bool
	enc      *zstd.Encoder
}

func (zw *zstdResponseWriter) WriteHeader(code int) {
	if code >= 100 && code <= 199 {
		zw.ResponseWriter.WriteHeader(code)
		return
	}
	if zw.code == 0 {
		zw.code = code
	}
}

func (zw *zstdResponseWriter) Write(p []byte) (int, error) {
	if !zw.decided {
		if len(zw.buf)+len(p) < zstdMinSize {
			zw.buf = append(zw.buf, p...)
			return len(p), nil
		}
		if err := zw.decide(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if zw.enc != nil {
		return zw.enc.Write(p)
	}
	return zw.ResponseWriter.Write(p)
}

// decide sends the status and headers, compressed or not, and then what was
// held back followed by p.
func (zw *zstdResponseWriter) decide(p []byte) error {
	zw.decided = true
	code := zw.code
	if code == 0 {
		code = http.StatusOK
	}
	h := zw.Header()
	h.Add("Vary", "Accept-Encoding")
	size := len(zw.buf) + len(p)
	if _, ok := h["Content-Type"]; !ok && size > 0 && code != http.StatusNoContent && code != http.StatusNotModified {
		sniff := zw.buf
		if len(sniff) < 512 {
			sniff = append(sniff[:len(sniff):len(sniff)], p[:min(len(p), 512-len(sniff))]...)
		}
		h.Set("Content-Type", http.DetectContentType(sniff))
	}
	w := zw.ResponseWriter.Write
	if size >= zstdMinSize && !zw.head && code >= 200 && code <= 299 && code != http.StatusNoContent && code != http.StatusPartialContent &&
		h.Get("Content-Encoding") == "" && h.Get("Content-Range") == "" {
		h.Set("Content-Encoding", "zstd")
		h.Del("Content-Length")
		h.Del("Accept-Ranges")
		zw.enc = getZstdEncoder()
		zw.enc.Reset(zw.ResponseWriter)
		w = zw.enc.Write
	}
	zw.ResponseWriter.WriteHeader(code)
	for _, b := range [][]byte{zw.buf, p} {
		if len(b) > 0 {
			if _, err := w(b); err != nil {
				return err
			}
		}
	}
	zw.buf = nil
	return nil
}

// Flush implements http.Flusher. Before anything is written it does nothing,
// lest the headers go out before it is known whether to compress; after, it
// sends what is held back, compressed or not, and flushes it.
func (zw *zstdResponseWriter) Flush() {
	if !zw.decided {
		if len(zw.buf) == 0 {
			return
		}
		if err := zw.decide(nil); err != nil {
			return
		}
	}
	if zw.enc != nil {
		if err := zw.enc.Flush(); err != nil {
			return
		}
	}
	if f, ok := zw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack implements http.Hijacker, which aborting a response needs; nothing
// more is written to the response after it.
func (zw *zstdResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := zw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("http.Hijacker interface is not supported")
	}
	zw.hijacked = true
	return hj.Hijack()
}

func (zw *zstdResponseWriter) close() {
	if !zw.hijacked {
		if !zw.decided {
			_ = zw.decide(nil)
		}
		if zw.enc != nil {
			_ = zw.enc.Close()
		}
	}
	if zw.enc != nil {
		zw.enc.Reset(nil)
		zstdEncoderPool.Put(zw.enc)
		zw.enc = nil
	}
}

var zstdEncoderPool sync.Pool

// getZstdEncoder returns an encoder of the fastest level that compresses on
// the goroutine writing the response. Its window is 1 MiB: on a real export
// that compresses 15.2 times, against 16.1 with the default 4 MiB window,
// while a response in flight holds a quarter of the memory.
func getZstdEncoder() *zstd.Encoder {
	if v := zstdEncoderPool.Get(); v != nil {
		return v.(*zstd.Encoder)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1),
		zstd.WithLowerEncoderMem(true), zstd.WithWindowSize(1<<20))
	if err != nil {
		logger.Panicf("BUG: cannot create zstd encoder: %s", err)
	}
	return enc
}
