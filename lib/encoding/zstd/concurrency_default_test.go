package zstd

import (
	"flag"
	"testing"
)

// The flag defaults to one slot rather than to the library's one-per-CPU, so a
// deployment gets the bounded footprint without carrying the flag in its
// arguments. That matters beyond the memory: a flag that has to be set to be
// useful is one a deployment can fail to carry across an upgrade, and passing a
// flag the binary does not know aborts start-up -- so an optional memory
// optimisation would take the process down. Nothing fails if this default goes
// back to 0; the process just quietly holds 8MB per CPU again.
//
// This is in the unconstrained file rather than next to the other flag tests,
// which are !cgo: the declaration is what both builds share.
func TestEncoderConcurrencyDefaultsToOneSlot(t *testing.T) {
	f := flag.Lookup("zstd.encoderConcurrency")
	if f == nil {
		t.Fatal("zstd.encoderConcurrency is not declared -- a cgo build would abort on start-up when it is passed")
	}
	if f.DefValue != "1" {
		t.Errorf("zstd.encoderConcurrency defaults to %q, want \"1\": at the library's own default every CPU costs another 8MB history window that a vmagent pushing a few hundred KB/s never uses", f.DefValue)
	}
}
