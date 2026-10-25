package remotewrite

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/streamaggr"
)

// A push that loaded the aggregators just before a config reload put new ones
// in their place and stopped them goes to the new ones: the stopped ones have
// had their last flush, and would lose its samples, or, matching nothing, have
// them kept as input.
func TestPushToStreamAggrFollowsAReload(t *testing.T) {
	cfg := []byte("- match: tokens\n  interval: 1m\n  outputs: [sum_samples]\n  flush_on_shutdown: true\n")
	var mu sync.Mutex
	var got []float64
	pushFunc := func(tss []prompb.TimeSeries) {
		mu.Lock()
		defer mu.Unlock()
		for _, ts := range tss {
			for _, s := range ts.Samples {
				got = append(got, s.Value)
			}
		}
	}
	load := func() *streamaggr.Aggregators {
		a, err := streamaggr.LoadFromData(cfg, pushFunc, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		return a
	}
	var p atomic.Pointer[streamaggr.Aggregators]
	p.Store(load())

	sas := p.Load() // the push has the aggregators of the config in place now
	p.Swap(load()).MustStop()

	tss := prometheus.MustParsePromMetrics("tokens 5", 1000)
	matchIdxs, enabled := pushToStreamAggr(&p, sas, tss, nil)
	if !enabled || len(matchIdxs) != 1 || matchIdxs[0] != 1 {
		t.Fatalf("the push was taken as not aggregated (enabled %v, matchIdxs %v): its input would be written as it is", enabled, matchIdxs)
	}
	p.Load().MustStop()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != 5 {
		t.Fatalf("written %v; want the push's 5 written once, by the aggregators of the new config", got)
	}
}
