package httpserver

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestPrefersZstd(t *testing.T) {
	f := func(want bool, acceptEncoding ...string) {
		t.Helper()
		if got := prefersZstd(acceptEncoding); got != want {
			t.Fatalf("prefersZstd(%q) = %v, want %v", acceptEncoding, got, want)
		}
	}
	f(false)
	f(false, "")
	f(true, "zstd")
	f(true, "zstd, gzip;q=0.5") // the ACG ruler
	f(true, "zstd, gzip")
	f(true, " ZSTD ; q=1 ,gzip")
	f(true, "br, zstd")
	f(false, "gzip, deflate, br, zstd") // browsers
	f(false, "*, zstd")
	f(false, "zstd;q=0, gzip")
	f(false, "zstd;Q=0, gzip")
	f(false, "zstd;q=bogus, gzip")
	f(false, "zstd;q=0.5, gzip")
	f(false, "zstd;q=0.1, gzip;q=1")
	f(true, "gzip;q=0.5, zstd")
	f(true, "*;q=0.1, zstd;q=1")
	f(true, "gzip;q=0", "zstd;q=0.1")
	f(true, "gzip;q=0.5", "zstd")
}

// serveCompressed serves handler through handlerWrapper and the compression
// wrapper, as serveWithListener does.
func serveCompressed(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(compressHandlerWrapper(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerWrapper(w, r, func(w http.ResponseWriter, r *http.Request) bool {
			handler(w, r)
			return true
		})
	})))
	t.Cleanup(srv.Close)
	return srv
}

// fetch requests url and returns the response with its body as sent.
func fetch(t *testing.T, method, url, acceptEncoding string, header ...string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	req.Header.Set("Accept-Encoding", acceptEncoding)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}

func unzstd(t *testing.T, b []byte) []byte {
	t.Helper()
	zr, err := zstd.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

const ruler = "zstd, gzip;q=0.5"

// TestZstdResponsesStream reads a response the handler writes in two parts,
// waiting after the first until the client has decoded it: a client that
// prefers zstd gets zstd, and gets it as it is written. Clients that do not
// get gzip or nothing, as before.
func TestZstdResponsesStream(t *testing.T) {
	first := bytes.Repeat([]byte("first part of the body\n"), 5000)
	second := bytes.Repeat([]byte("second part\n"), 5000)
	read := make(chan struct{})
	srv := httptest.NewServer(compressHandlerWrapper(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(first)
		w.(http.Flusher).Flush()
		if r.Header.Get("X-Wait") != "" {
			select {
			case <-read:
			case <-time.After(10 * time.Second):
			}
		}
		_, _ = w.Write(second)
	})))
	defer srv.Close()

	get := func(acceptEncoding string, wait bool) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
		req.Header.Set("Accept-Encoding", acceptEncoding)
		if wait {
			req.Header.Set("X-Wait", "1")
		}
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := get("zstd, gzip", true)
	if ce := resp.Header.Get("Content-Encoding"); ce != "zstd" {
		t.Fatalf("Content-Encoding %q, want zstd", ce)
	}
	zr, err := zstd.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(first))
	done := make(chan error, 1)
	go func() { _, err := io.ReadFull(zr, got); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first part did not come before the handler went on")
	}
	close(read)
	rest, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(got, rest...), append(append([]byte{}, first...), second...)) {
		t.Fatal("the zstd body differs from what was written")
	}
	zr.Close()
	resp.Body.Close()

	for _, c := range []struct{ acceptEncoding, want string }{{"gzip, deflate, br, zstd", "gzip"}, {"", ""}} {
		resp := get(c.acceptEncoding, false)
		if ce := resp.Header.Get("Content-Encoding"); ce != c.want {
			t.Fatalf("Accept-Encoding %q: Content-Encoding %q, want %q", c.acceptEncoding, ce, c.want)
		}
		var body io.Reader = resp.Body
		if c.want == "gzip" {
			gr, err := gzip.NewReader(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			body = gr
		}
		b, err := io.ReadAll(body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, append(append([]byte{}, first...), second...)) {
			t.Fatalf("Accept-Encoding %q: the body differs from what was written", c.acceptEncoding)
		}
		resp.Body.Close()
	}
}

// TestErrorsAndSmallBodiesAreSentAsTheyAre answers the ruler with a limit
// error of the size vmselect sends, a long error, and a small success: none is
// compressed, and the errors read as text; a big success is compressed.
func TestErrorsAndSmallBodiesAreSentAsTheyAre(t *testing.T) {
	limit := `error when exporting data: cannot fetch data: the number of matching timeseries exceeds 300000; either narrow down ` +
		`the search or increase -search.max* command-line flag values at vmselect; see https://docs.victoriametrics.com/victoriametrics/` +
		`single-server-victoriametrics/#resource-usage-limits` + strings.Repeat(" ", 250)
	big := strings.Repeat("a line of an export\n", 1000)
	for _, c := range []struct {
		code int
		body string
		zstd bool
	}{{422, limit, false}, {503, strings.Repeat(limit, 10), false}, {200, "OK", false}, {200, big, true}} {
		srv := serveCompressed(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(c.code)
			_, _ = io.WriteString(w, c.body)
		})
		resp, b := fetch(t, http.MethodGet, srv.URL, ruler)
		if got := resp.Header.Get("Content-Encoding") == "zstd"; got != c.zstd || resp.StatusCode != c.code {
			t.Fatalf("status %d, %d bytes: sent %d with Content-Encoding %q", c.code, len(c.body), resp.StatusCode, resp.Header.Get("Content-Encoding"))
		}
		if c.zstd {
			b = unzstd(t, b)
		}
		if string(b) != c.body {
			t.Fatalf("status %d: the body differs from what was written", c.code)
		}
	}
}

// TestTheRequestIsLeftAsItCame serves a handler that looks at Accept-Encoding,
// as a proxy does to pass it on: it sees what the client sent.
func TestTheRequestIsLeftAsItCame(t *testing.T) {
	seen := make(chan string, 1)
	srv := serveCompressed(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("Accept-Encoding")
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	})
	resp, _ := fetch(t, http.MethodGet, srv.URL, ruler)
	if ae := <-seen; ae != ruler || resp.Header.Get("Content-Encoding") != "zstd" {
		t.Fatalf("the handler saw Accept-Encoding %q, the client got %q", ae, resp.Header.Get("Content-Encoding"))
	}
	if vary := resp.Header.Values("Vary"); len(vary) != 1 || vary[0] != "Accept-Encoding" {
		t.Fatalf("Vary %q, want Accept-Encoding once", vary)
	}
}

// TestRangesAreNotCompressed asks a file server for part of a file and for
// all of it: the part is sent as it is, the whole compressed and without
// Accept-Ranges, which would promise ranges of the compressed bytes.
func TestRangesAreNotCompressed(t *testing.T) {
	files := http.FileServerFS(fstest.MapFS{"f.txt": {Data: []byte(strings.Repeat("0123456789\n", 3000))}})
	srv := serveCompressed(t, files.ServeHTTP)
	resp, b := fetch(t, http.MethodGet, srv.URL+"/f.txt", ruler, "Range", "bytes=0-4999")
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Encoding") != "" || len(b) != 5000 {
		t.Fatalf("a range: status %d, Content-Encoding %q, %d bytes", resp.StatusCode, resp.Header.Get("Content-Encoding"), len(b))
	}
	resp, b = fetch(t, http.MethodGet, srv.URL+"/f.txt", ruler)
	if resp.Header.Get("Content-Encoding") != "zstd" || resp.Header.Get("Accept-Ranges") != "" || len(unzstd(t, b)) != 33000 {
		t.Fatalf("the whole file: Content-Encoding %q, Accept-Ranges %q", resp.Header.Get("Content-Encoding"), resp.Header.Get("Accept-Ranges"))
	}
}

// TestFlushesAndEmptyWritesBeforeTheBody flush and write nothing before a big
// body, as a proxy may: the body is still compressed.
func TestFlushesAndEmptyWritesBeforeTheBody(t *testing.T) {
	body := strings.Repeat("a line of an export\n", 1000)
	for name, before := range map[string]func(w http.ResponseWriter){
		"a flush":        func(w http.ResponseWriter) { w.(http.Flusher).Flush() },
		"an empty write": func(w http.ResponseWriter) { _, _ = w.Write(nil) },
	} {
		srv := serveCompressed(t, func(w http.ResponseWriter, r *http.Request) {
			before(w)
			_, _ = io.WriteString(w, body)
		})
		resp, b := fetch(t, http.MethodGet, srv.URL, ruler)
		if resp.Header.Get("Content-Encoding") != "zstd" || string(unzstd(t, b)) != body {
			t.Fatalf("%s before the body: Content-Encoding %q", name, resp.Header.Get("Content-Encoding"))
		}
	}
}

// TestHeadAndContentTypeLeftOut answers HEAD without compressing it, and
// leaves out Content-Type where the handler asked for none.
func TestHeadAndContentTypeLeftOut(t *testing.T) {
	srv := serveCompressed(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/none" {
			w.Header()["Content-Type"] = nil
		}
		_, _ = io.WriteString(w, strings.Repeat("<html>", 1000))
	})
	resp, _ := fetch(t, http.MethodHead, srv.URL, ruler)
	if resp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("HEAD: Content-Encoding %q", resp.Header.Get("Content-Encoding"))
	}
	resp, _ = fetch(t, http.MethodGet, srv.URL+"/none", ruler)
	if ct, ok := resp.Header["Content-Type"]; ok || resp.Header.Get("Content-Encoding") != "zstd" {
		t.Fatalf("no Content-Type asked for: Content-Type %q, Content-Encoding %q", ct, resp.Header.Get("Content-Encoding"))
	}
}

// TestInformationalStatusesGoFirst sends an early hint before a big body: the
// hint goes out, and the response after it is compressed.
func TestInformationalStatusesGoFirst(t *testing.T) {
	body := strings.Repeat("a line of an export\n", 1000)
	srv := serveCompressed(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusEarlyHints)
		_, _ = io.WriteString(w, body)
	})
	resp, b := fetch(t, http.MethodGet, srv.URL, ruler)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Encoding") != "zstd" || string(unzstd(t, b)) != body {
		t.Fatalf("after an early hint: status %d, Content-Encoding %q", resp.StatusCode, resp.Header.Get("Content-Encoding"))
	}
}

// TestAnAbortedResponseLeavesAGoodEncoder aborts a compressed response half
// way, as vmselect does on an error: the client sees the body fail, nothing
// more is written to the taken over connection, and the next response, which
// may reuse the encoder, decodes.
func TestAnAbortedResponseLeavesAGoodEncoder(t *testing.T) {
	body := strings.Repeat("a line of an export\n", 20000)
	handler := compressHandlerWrapper(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerWrapper(w, r, func(w http.ResponseWriter, r *http.Request) bool {
			_, _ = io.WriteString(w, body[:len(body)/2])
			w.(http.Flusher).Flush()
			if r.URL.Path == "/abort" {
				Errorf(w, r, "cannot fetch the rest")
				return true
			}
			_, _ = io.WriteString(w, body[len(body)/2:])
			return true
		})
	}))
	aborted := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
		if r.URL.Path == "/abort" {
			close(aborted)
		}
	}))
	var serverLog bytes.Buffer
	srv.Config.ErrorLog = log.New(&serverLog, "", 0)
	srv.Start()
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/abort", nil)
	req.Header.Set("Accept-Encoding", ruler)
	if resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req); err == nil {
		if _, err := io.ReadAll(resp.Body); err == nil {
			t.Fatal("an aborted response read to its end")
		}
		resp.Body.Close()
	}
	<-aborted
	if strings.Contains(serverLog.String(), "hijacked") {
		t.Fatalf("written to after the abort: %s", serverLog.String())
	}
	for i := 0; i < 3; i++ {
		resp, b := fetch(t, http.MethodGet, srv.URL, ruler)
		if resp.Header.Get("Content-Encoding") != "zstd" || string(unzstd(t, b)) != body {
			t.Fatalf("response %d after an abort does not decode", i)
		}
	}
}

// TestTheWindowIsSmall checks the window a compressed response declares.
func TestTheWindowIsSmall(t *testing.T) {
	srv := serveCompressed(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("a line of an export\n", 300000))
	})
	_, b := fetch(t, http.MethodGet, srv.URL, ruler)
	var h zstd.Header
	if err := h.Decode(b); err != nil || h.WindowSize > 1<<20 {
		t.Fatalf("window %d bytes (%v), want at most 1 MiB", h.WindowSize, err)
	}
}
