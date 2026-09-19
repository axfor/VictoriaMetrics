package promutil

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// Sort has to order labels exactly as sorting by name does, including when names repeat.
func TestLabelsSortMatchesSortSlice(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	names := []string{"a", "b", "c", "instance", "job", "pod", "__name__", "a"}
	for i := 0; i < 2000; i++ {
		n := rnd.Intn(len(names) + 1)
		labels := make([]prompb.Label, 0, n)
		for j := 0; j < n; j++ {
			labels = append(labels, prompb.Label{
				Name:  names[rnd.Intn(len(names))],
				Value: string(rune('A' + rnd.Intn(4))),
			})
		}
		want := append([]prompb.Label{}, labels...)
		sort.SliceStable(want, func(i, j int) bool { return want[i].Name < want[j].Name })

		x := &Labels{Labels: append([]prompb.Label{}, labels...)}
		x.Sort()
		if len(x.Labels) != len(want) {
			t.Fatalf("length changed: got %d, want %d", len(x.Labels), len(want))
		}
		for j := range want {
			if x.Labels[j] != want[j] {
				t.Fatalf("input %v: got %v, want %v", labels, x.Labels, want)
			}
		}
	}
}
