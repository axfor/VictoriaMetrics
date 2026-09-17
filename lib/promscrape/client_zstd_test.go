package promscrape

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/chunkedbuffer"
)

// testScrapeBody returns a few MiB of exposition-format text, so zstd and gzip produce multi-block streams.
func testScrapeBody() string {
	var sb strings.Builder
	for i := 0; i < 60000; i++ {
		fmt.Fprintf(&sb, `acg_requests_total{apikey_id="key-%08d-aaaa-bbbb-cccc",route_model_id="route-%d",business_group_id="bg-%d"} %d`+"\n", i, i%5, i%50, i*7)
	}
	return sb.String()
}

func TestClientReadDataContentEncoding(t *testing.T) {
	body := testScrapeBody()

	f := func(zstdEnabled bool, respEncoding, wantAcceptEncoding, wantContentEncoding string) {
		t.Helper()

		origZstd := *zstdCompression
		*zstdCompression = zstdEnabled
		defer func() {
			*zstdCompression = origZstd
		}()

		var gotAcceptEncoding string
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAcceptEncoding = r.Header.Get("Accept-Encoding")
			if respEncoding != "" {
				w.Header().Set("Content-Encoding", respEncoding)
			}
			switch respEncoding {
			case "zstd":
				// The same streaming encoder the scrape targets use.
				zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
				if err != nil {
					panic(err)
				}
				_, _ = zw.Write([]byte(body))
				_ = zw.Close()
			case "gzip":
				zw := gzip.NewWriter(w)
				_, _ = zw.Write([]byte(body))
				_ = zw.Close()
			default:
				_, _ = w.Write([]byte(body))
			}
		}))
		defer backend.Close()

		c, err := newClient(context.Background(), &ScrapeWork{
			ScrapeURL:     backend.URL,
			ScrapeTimeout: 5 * time.Second,
			AuthConfig:    newTestAuthConfig(t, false, nil),
			MaxScrapeSize: 64 << 20,
		})
		if err != nil {
			t.Fatalf("cannot create client: %s", err)
		}

		var cb chunkedbuffer.Buffer
		contentEncoding, err := c.ReadData(&cb)
		if err != nil {
			t.Fatalf("unexpected error at ReadData: %s", err)
		}
		if gotAcceptEncoding != wantAcceptEncoding {
			t.Fatalf("unexpected Accept-Encoding; got %q; want %q", gotAcceptEncoding, wantAcceptEncoding)
		}
		if contentEncoding != wantContentEncoding {
			t.Fatalf("unexpected content encoding; got %q; want %q", contentEncoding, wantContentEncoding)
		}

		var bb bytesutil.ByteBuffer
		if err := readFromBuffer(&bb, &cb, contentEncoding); err != nil {
			t.Fatalf("cannot read response body: %s", err)
		}
		if string(bb.B) != body {
			t.Fatalf("unexpected response body; got %d bytes; want %d bytes", len(bb.B), len(body))
		}
	}

	// zstd is disabled by default: the request and the response handling stay as in upstream.
	f(false, "gzip", "gzip", "gzip")
	f(false, "", "gzip", "")

	// zstd is enabled and the target supports it.
	f(true, "zstd", "zstd, gzip", "zstd")

	// zstd is enabled, but the target supports only gzip.
	f(true, "gzip", "zstd, gzip", "gzip")

	// zstd is enabled, but the target doesn't compress responses.
	f(true, "", "zstd, gzip", "")
}
