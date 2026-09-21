package zstd

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// The encoder is created without WithEncoderConcurrency, so klauspost defaults
// it to GOMAXPROCS and each of those goroutines keeps an 8MB history window.
// On a vmagent profiled at 30k keys that was 80MB on ten cores, the largest
// single item in its heap, and it scales with the core count rather than with
// how much there is to compress: 512MB on a 64 core node.
//
// This measures what the concurrency actually buys, on blocks shaped like the
// ones remote write sends.
func BenchmarkCompressLevelConcurrency(b *testing.B) {
	// A remote write block: protobuf-ish repetitive bytes, a few hundred KB.
	block := make([]byte, 512<<10)
	for i := range block {
		block[i] = byte("abcdefghijklmnop"[i%16] + byte(i/4096%7))
	}

	for _, n := range []int{1, 2, 4, runtime.GOMAXPROCS(0)} {
		b.Run(fmt.Sprintf("concurrency=%d", n), func(b *testing.B) {
			e, err := zstd.NewWriter(nil,
				zstd.WithEncoderCRC(false),
				zstd.WithEncoderLevel(zstd.SpeedDefault),
				zstd.WithEncoderConcurrency(n))
			if err != nil {
				b.Fatal(err)
			}
			defer e.Close()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(len(block)))
			var dst []byte
			for range b.N {
				dst = e.EncodeAll(block, dst[:0])
			}
			b.ReportMetric(float64(len(dst)), "compressed_bytes")
		})
	}
}

// And the same under contention, which is what a concurrency setting is for.
func BenchmarkCompressLevelConcurrencyParallel(b *testing.B) {
	block := make([]byte, 512<<10)
	for i := range block {
		block[i] = byte("abcdefghijklmnop"[i%16] + byte(i/4096%7))
	}

	for _, n := range []int{1, 2, 4, runtime.GOMAXPROCS(0)} {
		b.Run(fmt.Sprintf("concurrency=%d", n), func(b *testing.B) {
			e, err := zstd.NewWriter(nil,
				zstd.WithEncoderCRC(false),
				zstd.WithEncoderLevel(zstd.SpeedDefault),
				zstd.WithEncoderConcurrency(n))
			if err != nil {
				b.Fatal(err)
			}
			defer e.Close()

			b.ResetTimer()
			b.SetBytes(int64(len(block)))
			b.RunParallel(func(pb *testing.PB) {
				var dst []byte
				for pb.Next() {
					dst = e.EncodeAll(block, dst[:0])
				}
			})
		})
	}
}
