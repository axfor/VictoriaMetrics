package streamaggr

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// What aggregation state costs per output series, on the configuration ACG runs
// and label sets of the shape it sends: ten labels, four of them UUIDs.
//
// A 30k-key deployment keeps about nine million output series in this state, so
// bytes per series is what decides vmagent's memory; a heap profile of one taken
// under that load put a third of it in sync.Map's trie nodes rather than in the
// data.
func TestACGAggregationMemoryPerSeries(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a few hundred MiB")
	}
	const n = 300000
	config := `
- match: '{_metric_type!="gauge",_metric_type!=""}'
  interval: 60s
  drop_input_labels: [_metric_type, pod, instance, node]
  outputs: [sum_samples_total]
  keep_metric_names: true
  staleness_interval: 40m
  reset_marker_on_stale: true
  flush_on_shutdown: true
  output_heartbeat_interval: 4m
`
	a, err := LoadFromData([]byte(config), func(_ []prompb.TimeSeries) {}, nil, "acg")
	if err != nil {
		t.Fatalf("cannot load config: %s", err)
	}
	defer a.MustStop()

	uuid := func(kind string, i int) string {
		return fmt.Sprintf("%08x-%04x-5%03x-8%03x-%012x", i*2654435761%0xffffffff, len(kind), i%0xfff, (i/7)%0xfff, i)
	}
	series := make([]prompb.TimeSeries, 0, 1000)
	var matchIdxs []byte
	push := func() {
		matchIdxs = a.Push(series, matchIdxs)
		series = series[:0]
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := range n {
		key, combo := i/4, i%4
		series = append(series, prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "acg_input_tokens"},
				{Name: "_metric_type", Value: "counter"},
				{Name: "apikey_id", Value: uuid("key", key)},
				{Name: "business_group", Value: fmt.Sprintf("bg-%03d", key%50)},
				{Name: "business_group_id", Value: uuid("bg", key%50)},
				{Name: "pod", Value: "model-router-764746947b-ct9hv"},
				{Name: "provider_id", Value: fmt.Sprintf("provider-%02d", combo)},
				{Name: "provider_model", Value: fmt.Sprintf("provider-%02d-model-%d", combo, combo)},
				{Name: "provider_model_id", Value: uuid("model", combo)},
				{Name: "route_model", Value: fmt.Sprintf("model-%03d", key%20)},
				{Name: "route_model_id", Value: uuid("route", key%20)},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: 1}},
		})
		if len(series) == cap(series) {
			push()
		}
	}
	push()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	perSeries := float64(after.HeapAlloc-before.HeapAlloc) / n
	objs := float64(after.HeapObjects-before.HeapObjects) / n
	t.Logf("%d output series: %.0f bytes and %.1f heap objects per series", n, perSeries, objs)
}
