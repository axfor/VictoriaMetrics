package promutil

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// CompressCached has to produce exactly what Compress produces, whether the cache
// hits, misses or collides.
func TestCompressCachedMatchesCompress(t *testing.T) {
	var lc LabelsCompressor
	var c CompressorCache
	rnd := rand.New(rand.NewSource(7))

	// More distinct labels than the cache has slots, so entries are evicted and reused.
	//
	// Names and values are drawn from small sets that cross over, so the same value
	// shows up under several names: a cache that compared only the value would hand
	// back the wrong index. The empty label is in there too, since an unused slot
	// holds exactly that and must not be taken for a hit.
	var pool []prompb.Label
	pool = append(pool, prompb.Label{})
	for i := 0; i < 4*compressorCacheSize; i++ {
		pool = append(pool, prompb.Label{
			Name:  fmt.Sprintf("name_%d", i%97),
			Value: fmt.Sprintf("value_%d", i%89),
		})
	}

	for round := 0; round < 3; round++ {
		for i := 0; i < 20000; i++ {
			n := rnd.Intn(9)
			labels := make([]prompb.Label, 0, n)
			for j := 0; j < n; j++ {
				labels = append(labels, pool[rnd.Intn(len(pool))])
			}
			want := lc.Compress(nil, labels)
			got := lc.CompressCached(nil, labels, &c)
			if string(got) != string(want) {
				t.Fatalf("round %d labels %v: cached %v, uncached %v", round, labels, got, want)
			}
			// and the result still decompresses back to the same labels
			back := lc.Decompress(nil, got)
			if len(back) != len(labels) {
				t.Fatalf("decompressed %v, want %v", back, labels)
			}
			for j := range labels {
				if back[j].Name != labels[j].Name || back[j].Value != labels[j].Value {
					t.Fatalf("decompressed %v, want %v", back, labels)
				}
			}
		}
	}
}

// A cache slot is chosen by hashing name and value together, so two labels can land
// in the same slot. When they also share a value, only the name tells them apart:
// this builds exactly that pair and checks both still compress correctly.
func TestCompressCachedSlotCollision(t *testing.T) {
	const value = "shared-value"
	var c CompressorCache

	var a, b prompb.Label
	found := false
	for i := 0; i < 1<<20 && !found; i++ {
		a = prompb.Label{Name: fmt.Sprintf("n%d", i), Value: value}
		for j := i + 1; j < i+4096; j++ {
			b = prompb.Label{Name: fmt.Sprintf("n%d", j), Value: value}
			if c.slot(a) == c.slot(b) {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatal("no colliding pair found")
	}
	t.Logf("colliding labels: %q and %q", a.Name, b.Name)

	var lc LabelsCompressor
	wantA := lc.Compress(nil, []prompb.Label{a})
	wantB := lc.Compress(nil, []prompb.Label{b})
	if string(wantA) == string(wantB) {
		t.Fatal("two distinct labels must not compress to the same bytes")
	}
	// Prime the slot with a, then ask for b: the slot is taken by a label with the
	// same value and a different name.
	if got := lc.CompressCached(nil, []prompb.Label{a}, &c); string(got) != string(wantA) {
		t.Fatalf("a: got %v, want %v", got, wantA)
	}
	if got := lc.CompressCached(nil, []prompb.Label{b}, &c); string(got) != string(wantB) {
		t.Fatalf("b: got %v, want %v", got, wantB)
	}
	// and back to a, whose slot b has just taken over
	if got := lc.CompressCached(nil, []prompb.Label{a}, &c); string(got) != string(wantA) {
		t.Fatalf("a again: got %v, want %v", got, wantA)
	}
}
