package promscrape

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/chunkedbuffer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
)

// pushSummary summarizes pushed samples as an order-independent hash, since stream parsing pushes blocks concurrently.
type pushSummary struct {
	mu      sync.Mutex
	samples int
	hashSum uint64
	auto    map[string]float64
}

func (ps *pushSummary) push(_ *auth.Token, wr *prompb.WriteRequest) {
	var buf []byte
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, ts := range wr.Timeseries {
		name := ""
		buf = buf[:0]
		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				name = l.Value
			}
			buf = append(buf, l.Name...)
			buf = append(buf, '=')
			buf = append(buf, l.Value...)
			buf = append(buf, ',')
		}
		if name == "up" || strings.HasPrefix(name, "scrape_") {
			ps.auto[name] = ts.Samples[0].Value
			continue
		}
		for _, s := range ts.Samples {
			b := strconv.AppendFloat(buf, s.Value, 'g', -1, 64)
			b = strconv.AppendInt(append(b, '@'), s.Timestamp, 10)
			ps.hashSum += xxhash.Sum64(b)
			ps.samples++
		}
	}
}

func scrapeSummary(t testing.TB, compressed []byte, contentEncoding string, cfg *ScrapeWork) (*pushSummary, error) {
	t.Helper()
	ps := &pushSummary{auto: map[string]float64{}}
	var sw scrapeWork
	sw.Config = cfg
	sw.ReadData = func(dst *chunkedbuffer.Buffer) (string, error) {
		dst.MustWrite(compressed)
		return contentEncoding, nil
	}
	sw.PushData = ps.push
	tsmGlobal.Register(&sw)
	defer tsmGlobal.Unregister(&sw)
	err := sw.scrapeInternal(123000, 123000)
	return ps, err
}

func compressBody(t testing.TB, body []byte, contentEncoding string) []byte {
	t.Helper()
	switch contentEncoding {
	case "zstd":
		return encoding.CompressZSTDLevel(nil, body, 1)
	case "gzip":
		var bb bytes.Buffer
		zw := gzip.NewWriter(&bb)
		if _, err := zw.Write(body); err != nil {
			t.Fatalf("cannot gzip: %s", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("cannot gzip: %s", err)
		}
		return bb.Bytes()
	default:
		return body
	}
}

// The path which decompresses and parses the read response block by block must push the same samples
// as the path which copies the whole uncompressed response into memory first.
func TestScrapeWorkStreamWithoutBody(t *testing.T) {
	protoparserutil.StartUnmarshalWorkers()
	defer protoparserutil.StopUnmarshalWorkers()

	var sb strings.Builder
	sb.WriteString("# HELP acg_requests_total requests\n# TYPE acg_requests_total counter\n")
	for i := range 100_000 {
		fmt.Fprintf(&sb, "acg_requests_total{apikey_id=\"k%d\",route=\"r%d\"} %d\n", i, i%7, i)
	}
	body := []byte(sb.String())

	for _, enc := range []string{"", "gzip", "zstd"} {
		compressed := compressBody(t, body, enc)
		// The whole response is kept in memory: stale markers are enabled.
		want, err := scrapeSummary(t, compressed, enc, &ScrapeWork{StreamParse: true, ScrapeTimeout: time.Minute})
		if err != nil {
			t.Fatalf("encoding %q: unexpected error: %s", enc, err)
		}
		for _, cfg := range []*ScrapeWork{
			{NoStaleMarkers: true, ScrapeTimeout: time.Minute},                    // switched by response size
			{NoStaleMarkers: true, StreamParse: true, ScrapeTimeout: time.Minute}, // forced
		} {
			got, err := scrapeSummary(t, compressed, enc, cfg)
			if err != nil {
				t.Fatalf("encoding %q: unexpected error: %s", enc, err)
			}
			if got.samples != 100_000 || got.samples != want.samples || got.hashSum != want.hashSum {
				t.Fatalf("encoding %q, StreamParse=%v: pushed samples differ; got %d samples, hash %x; want %d samples, hash %x",
					enc, cfg.StreamParse, got.samples, got.hashSum, want.samples, want.hashSum)
			}
			for _, name := range []string{"up", "scrape_samples_scraped", "scrape_response_size_bytes"} {
				if got.auto[name] != want.auto[name] {
					t.Fatalf("encoding %q, StreamParse=%v: %s = %v; want %v", enc, cfg.StreamParse, name, got.auto[name], want.auto[name])
				}
			}
			if got.auto["scrape_response_size_bytes"] != float64(len(body)) {
				t.Fatalf("encoding %q: scrape_response_size_bytes must be the uncompressed size %d; got %v", enc, len(body), got.auto["scrape_response_size_bytes"])
			}
		}
	}
}

// A decompression error in the middle of the response marks the scrape as failed.
// gzip is used, since the zstd reader returns io.EOF for a truncated frame on both paths (upstream behavior).
func TestScrapeWorkStreamWithoutBodyBrokenData(t *testing.T) {
	protoparserutil.StartUnmarshalWorkers()
	defer protoparserutil.StopUnmarshalWorkers()

	body := []byte(strings.Repeat("foo{bar=\"baz\"} 1\n", 100_000))
	compressed := compressBody(t, body, "gzip")
	compressed = compressed[:len(compressed)/2]
	got, err := scrapeSummary(t, compressed, "gzip", &ScrapeWork{NoStaleMarkers: true, StreamParse: true, ScrapeTimeout: time.Minute})
	if err == nil {
		t.Fatalf("expecting an error for a truncated gzip response")
	}
	if got.auto["up"] != 0 || got.auto["scrape_response_size_bytes"] != 0 {
		t.Fatalf("a failed scrape must report up=0 and zero response size; got %v", got.auto)
	}
}

func TestScrapeWorkCanStreamWithoutBody(t *testing.T) {
	f := func(cfg *ScrapeWork, prevBodyLen, readSize int, want bool) {
		t.Helper()
		sw := &scrapeWork{Config: cfg, prevBodyLen: prevBodyLen}
		if got := sw.canStreamWithoutBody(readSize); got != want {
			t.Fatalf("cfg %+v, prevBodyLen=%d, readSize=%d: got %v; want %v", cfg, prevBodyLen, readSize, got, want)
		}
	}
	const big = 2e6
	// stale markers and series_limit need the whole response
	f(&ScrapeWork{StreamParse: true}, big, big, false)
	f(&ScrapeWork{NoStaleMarkers: true, SeriesLimit: 10, StreamParse: true}, big, big, false)
	// forced stream parsing
	f(&ScrapeWork{NoStaleMarkers: true, StreamParse: true}, 0, 10, true)
	// automatic switching by size
	f(&ScrapeWork{NoStaleMarkers: true}, 0, 10, false)
	f(&ScrapeWork{NoStaleMarkers: true}, big, 10, true)
	f(&ScrapeWork{NoStaleMarkers: true}, 0, big, true)
	f(&ScrapeWork{NoStaleMarkers: true, SampleLimit: 10}, big, big, false)
}

// TestScrapeWorkStreamWithoutBodyRealResponse compares both paths on a real compressed response and reports peak heap.
// Set ACG_SCRAPE_BODY to a zstd-compressed /metrics response to run it.
func TestScrapeWorkStreamWithoutBodyRealResponse(t *testing.T) {
	path := os.Getenv("ACG_SCRAPE_BODY")
	if path == "" {
		t.Skip("ACG_SCRAPE_BODY isn't set")
	}
	compressed, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read %s: %s", path, err)
	}
	protoparserutil.StartUnmarshalWorkers()
	defer protoparserutil.StopUnmarshalWorkers()

	run := func(name string, cfg *ScrapeWork) *pushSummary {
		runtime.GC()
		runtime.GC()
		base := heapBytes()
		var peak atomic.Uint64
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				if h := heapBytes(); h > peak.Load() {
					peak.Store(h)
				}
				select {
				case <-stop:
					return
				case <-time.After(2 * time.Millisecond):
				}
			}
		}()
		start := time.Now()
		ps, err := scrapeSummary(t, compressed, "zstd", cfg)
		d := time.Since(start)
		close(stop)
		<-done
		if err != nil {
			t.Fatalf("%s: unexpected error: %s", name, err)
		}
		t.Logf("%s: %d samples, response %.1f MiB, took %s, peak heap above baseline %.0f MiB",
			name, ps.samples, ps.auto["scrape_response_size_bytes"]/(1<<20), d.Round(time.Millisecond), float64(peak.Load()-min(peak.Load(), base))/(1<<20))
		return ps
	}
	// Run the new path first, so pools warmed up by the old path don't hide its allocations.
	got := run("stream without body", &ScrapeWork{NoStaleMarkers: true, ScrapeTimeout: time.Minute})
	want := run("copy whole body", &ScrapeWork{NoStaleMarkers: false, StreamParse: true, ScrapeTimeout: time.Minute})
	if got.samples != want.samples || got.hashSum != want.hashSum || got.auto["scrape_response_size_bytes"] != want.auto["scrape_response_size_bytes"] {
		t.Fatalf("pushed samples differ; got %d samples, hash %x; want %d samples, hash %x", got.samples, got.hashSum, want.samples, want.hashSum)
	}
}

func heapBytes() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}
