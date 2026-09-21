//go:build !cgo

package zstd

import (
	"flag"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/klauspost/compress/zstd"
)

var encoderConcurrency = flag.Int("zstd.encoderConcurrency", 0, "How many blocks may be zstd-compressed at once. "+
	"Each one keeps an 8MB history window for the life of the process, so this is also 8MB of memory apiece. "+
	"0 means one per CPU, which is what the compression library defaults to: on a 64 CPU node that is 512MB held "+
	"whether or not there is anything to compress. Compressing a single block does not use more than one of them "+
	"-- the concurrency only helps when several blocks are compressed at the same time -- so a process that "+
	"compresses a few hundred KB/s has no use for more than one or two. Measured on one block at a time, "+
	"concurrency makes no difference (4.2GB/s either way); with every goroutine compressing, throughput scales "+
	"with it (4.2, 8.4, 16.6, 33.0 GB/s at 1, 2, 4 and 10)")

var (
	decoder *zstd.Decoder

	mu sync.Mutex

	// do not use atomic.Pointer, since the stored map there is already a pointer type.
	av atomic.Value
)

func init() {
	r := make(map[zstd.EncoderLevel]*zstd.Encoder)
	av.Store(r)

	var err error
	decoder, err = zstd.NewReader(nil)
	if err != nil {
		logger.Panicf("BUG: failed to create ZSTD reader: %s", err)
	}
}

// Decompress appends decompressed src to dst and returns the result.
func Decompress(dst, src []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}

// CompressLevel appends compressed src to dst and returns the result.
//
// The given compressionLevel is used for the compression.
func CompressLevel(dst, src []byte, compressionLevel int) []byte {
	// Convert the compressionLevel to the real compression level supported by github.com/klauspost/compress/zstd
	// This allows saving memory on caching zstd.Encoder instances per each level,
	// since the number of real compression levels at github.com/klauspost/compress/zstd
	// is smaller than the number of zstd compression levels.
	// See https://github.com/klauspost/compress/discussions/1025
	realCompressionLevel := zstd.EncoderLevelFromZstd(compressionLevel)

	e := getEncoder(realCompressionLevel)
	return e.EncodeAll(src, dst)
}

func getEncoder(compressionLevel zstd.EncoderLevel) *zstd.Encoder {
	r := av.Load().(map[zstd.EncoderLevel]*zstd.Encoder)
	e := r[compressionLevel]
	if e != nil {
		return e
	}

	mu.Lock()
	// Create the encoder under lock in order to prevent from wasted work
	// when concurrent goroutines create encoder for the same compressionLevel.
	r1 := av.Load().(map[zstd.EncoderLevel]*zstd.Encoder)
	if e = r1[compressionLevel]; e == nil {
		e = newEncoder(compressionLevel)
		r2 := make(map[zstd.EncoderLevel]*zstd.Encoder)
		for k, v := range r1 {
			r2[k] = v
		}
		r2[compressionLevel] = e
		av.Store(r2)
	}
	mu.Unlock()

	return e
}

// resolveEncoderConcurrency is the flag, or the library's own default when it
// is unset or nonsense.
func resolveEncoderConcurrency() int {
	if n := *encoderConcurrency; n > 0 {
		return n
	}
	return runtime.GOMAXPROCS(0)
}

func newEncoder(compressionLevel zstd.EncoderLevel) *zstd.Encoder {
	concurrency := resolveEncoderConcurrency()
	e, err := zstd.NewWriter(nil,
		zstd.WithEncoderCRC(false), // Disable CRC for performance reasons.
		zstd.WithEncoderLevel(compressionLevel),
		zstd.WithEncoderConcurrency(concurrency))
	if err != nil {
		logger.Panicf("BUG: failed to create ZSTD writer: %s", err)
	}
	return e
}
