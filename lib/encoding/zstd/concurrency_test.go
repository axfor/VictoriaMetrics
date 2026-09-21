//go:build !cgo

package zstd

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// The encoder keeps an 8MB history window per concurrent slot for the life of
// the process, and the library defaults that count to GOMAXPROCS -- on a 64 CPU
// node, 512MB held whether or not there is anything to compress.
// -zstd.encoderConcurrency bounds it. Nothing fails when it is wrong, the
// process just holds the memory, so the wiring is asserted here.
func TestEncoderConcurrencyFlag(t *testing.T) {
	orig := *encoderConcurrency
	t.Cleanup(func() { *encoderConcurrency = orig })

	for _, tc := range []struct {
		set  int
		want int
	}{
		{0, runtime.GOMAXPROCS(0)}, // unset keeps the library's default
		{1, 1},
		{3, 3},
		{-5, runtime.GOMAXPROCS(0)}, // nonsense falls back rather than failing
	} {
		*encoderConcurrency = tc.set
		got := resolveEncoderConcurrency()
		if got != tc.want {
			t.Errorf("-zstd.encoderConcurrency=%d resolved to %d, want %d", tc.set, got, tc.want)
		}
	}
}

// Whatever the concurrency, the bytes have to survive a round trip unchanged.
// A wrong setting here would corrupt every block vmagent sends.
func TestCompressRoundTripAtEveryConcurrency(t *testing.T) {
	orig := *encoderConcurrency
	t.Cleanup(func() { *encoderConcurrency = orig })

	payload := bytes.Repeat([]byte("acg_requests_total{apikey_id=\"k-1234\"} 42\n"), 5000)
	for _, n := range []int{1, 2, 4, 0} {
		*encoderConcurrency = n
		e := newEncoder(zstd.SpeedDefault)
		compressed := e.EncodeAll(payload, nil)
		e.Close()

		got, err := Decompress(nil, compressed)
		if err != nil {
			t.Fatalf("concurrency %d: cannot decompress: %s", n, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("concurrency %d: round trip changed the bytes (%d in, %d out)", n, len(payload), len(got))
		}
	}
}
