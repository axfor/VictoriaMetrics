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

// What an exposer's label ordering costs the ingest path. A sidecar that renders
// its generation label after the metric's own labels leaves one name out of
// place at the end of every series; rendering it where it sorts leaves nothing
// to move. The label set is the one the API-key sidecars produce.
func BenchmarkLabelsSortExposerOrdering(b *testing.B) {
	own := []prompb.Label{
		{Name: "__name__", Value: "acg_requests_total"},
		{Name: "apikey_id", Value: "7ab2d570-b30d-51c9-aa87-637cbb99a8f2"},
		{Name: "business_group", Value: "bg-045"},
		{Name: "business_group_id", Value: "595c8d9b-d0f9-51cd-881e-aac13083337b"},
		{Name: "provider_id", Value: "provider-00"},
		{Name: "provider_model", Value: "provider-00-model-0"},
		{Name: "provider_model_id", Value: "2b4e65c3-600d-58ae-b382-7b84a5c71e2d"},
		{Name: "route_model", Value: "model-000"},
		{Name: "route_model_id", Value: "0f74c5ca-55cb-5fff-a37f-30782cf25609"},
	}
	gen := prompb.Label{Name: "gen", Value: "1789837285"}

	appended := append(append([]prompb.Label{}, own...), gen)
	inPlace := append([]prompb.Label{}, own...)
	inPlace = append(inPlace, prompb.Label{})
	copy(inPlace[5:], inPlace[4:])
	inPlace[4] = gen

	sorted := func(labels []prompb.Label) bool {
		for i := 1; i < len(labels); i++ {
			if labels[i-1].Name > labels[i].Name {
				return false
			}
		}
		return true
	}
	if !sorted(inPlace) || sorted(appended) {
		b.Fatalf("the two orderings are not what this benchmark compares:\n%v\n%v", inPlace, appended)
	}

	for _, tc := range []struct {
		name string
		in   []prompb.Label
	}{
		{"generation_appended", appended},
		{"generation_sorted", inPlace},
	} {
		b.Run(tc.name, func(b *testing.B) {
			x := &Labels{Labels: make([]prompb.Label, len(tc.in))}
			b.ReportAllocs()
			for b.Loop() {
				copy(x.Labels, tc.in)
				x.Sort()
			}
		})
	}
}
