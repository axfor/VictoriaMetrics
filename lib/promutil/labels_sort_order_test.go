package promutil

import (
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// Sort is hand-written rather than going through sort.Interface, and nothing
// checked that it orders the same way. It sits on the stream aggregation push
// path where a wrong order does not fail: it changes the key a sample groups
// under, so the samples land in the wrong output series and the numbers are
// quietly wrong. So the order is checked against the standard library on names
// built to hit the edges -- shared prefixes, one name extending another, and
// names differing only in their tail.
func TestSortMatchesStandardSort(t *testing.T) {
	names := []string{
		"a", "ab", "abc", "abcdefg", "abcdefgh", "abcdefghi", "abcdefghj",
		"abcdefgh0", "abcdefgh_", "label1", "label2", "label3", "label10",
		"__name__", "__name___", "_", "z", "job", "instance", "pod", "namespace",
		"node", "environment", "path", "some_other_label", "A", "Z", "aZ", "a_",
	}
	rng := rand.New(rand.NewSource(20260920))

	for round := range 20000 {
		n := 1 + rng.Intn(20)
		labels := make([]prompb.Label, n)
		seen := map[string]bool{}
		for i := range labels {
			var name string
			for {
				name = names[rng.Intn(len(names))]
				if rng.Intn(4) == 0 {
					name += strconv.Itoa(rng.Intn(3))
				}
				if !seen[name] {
					break
				}
			}
			seen[name] = true
			labels[i] = prompb.Label{Name: name, Value: "v" + strconv.Itoa(i)}
		}

		want := append([]prompb.Label(nil), labels...)
		sort.SliceStable(want, func(i, j int) bool { return want[i].Name < want[j].Name })

		x := &Labels{Labels: append([]prompb.Label(nil), labels...)}
		x.Sort()

		for i := range want {
			if x.Labels[i].Name != want[i].Name {
				t.Fatalf("round %d: at %d got %q, want %q\n  input %v", round, i, x.Labels[i].Name, want[i].Name, labels)
			}
		}
	}
}
