package zstd

import "flag"

// encoderConcurrency bounds how many blocks the pure-Go encoder may compress at
// once. Each slot keeps an 8MB history window for the life of the process, and
// the library defaults the count to GOMAXPROCS -- on a 64 CPU node that is
// 512MB held whether or not there is anything to compress. Compressing a single
// block does not use more than one slot; the concurrency only helps when
// several blocks are compressed at the same time, so a process pushing a few
// hundred KB/s has no use for more than one or two.
//
// The default is one slot rather than the library's own one-per-CPU, so that a
// deployment gets the bounded footprint without having to set anything. A flag
// that has to be set to be useful is a flag a deployment can fail to carry
// across an upgrade, and passing one the binary does not know aborts start-up --
// a hard failure out of an optional memory optimisation. Raise it only for a
// vmagent that really does compress several blocks at once.
//
// It is declared for both builds so that passing it never aborts start-up. The
// cgo build compresses through gozstd, which has no such pool, and ignores it.
var encoderConcurrency = flag.Int("zstd.encoderConcurrency", 1, "How many blocks may be zstd-compressed at once. "+
	"Each one keeps an 8MB history window for the life of the process, so this is also 8MB of memory apiece. "+
	"The default is 1; 0 means one per CPU, which is what the compression library itself defaults to. Compressing "+
	"a single block does not use more than one of them -- the concurrency only helps when several blocks are "+
	"compressed at the same time -- so a process that compresses a few hundred KB/s has no use for more than one "+
	"or two. Has no effect in cgo builds, which compress through gozstd")
