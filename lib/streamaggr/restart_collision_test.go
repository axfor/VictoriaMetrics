package streamaggr

import (
	"sort"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
)

// The leading zero of a new output lands one interval before its first flush, which is close to
// where the process that died may have flushed a real value. Two samples of the same series at the
// same timestamp are resolved by keeping the larger one, which would swallow the zero and hide the
// reset, so they must never collide.
//
// They cannot: the first incomplete interval after a start is not flushed, which leaves at least one
// interval between the last flush of the old process and the leading zero of the new one. This pins
// that down with the tightest restart there is, one that lives through a boundary and comes back
// within the same interval.
func TestLeadingZeroAfterShortRestart(t *testing.T) {
	const config = `
- interval: 1m
  staleness_interval: 5m
  ignore_first_sample_interval: 0s
  without: [pod]
  outputs: [total]
  keep_metric_names: true
  reset_marker_on_stale: true
`
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var tss []prompb.TimeSeries
		pushFunc := func(src []prompb.TimeSeries) {
			mu.Lock()
			tss = appendClonedTimeseries(tss, src)
			mu.Unlock()
		}
		start := time.Now()
		a, err := LoadFromData([]byte(config), pushFunc, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		time.Sleep(10 * time.Second)
		a.Push(prometheus.MustParsePromMetrics(`foo{pod="a"} 5`, time.Now().UnixMilli()), nil)
		time.Sleep(175 * time.Second) // live through the 180s boundary, then die just after it
		a.MustStop()

		time.Sleep(5 * time.Second) // a short restart, well under one interval
		b, err := LoadFromData([]byte(config), pushFunc, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		b.Push(prometheus.MustParsePromMetrics(`foo{pod="b"} 5`, time.Now().UnixMilli()), nil)
		time.Sleep(150 * time.Second)
		b.MustStop()

		mu.Lock()
		defer mu.Unlock()
		var out []markerSample
		for _, ts := range tss {
			for _, s := range ts.Samples {
				out = append(out, markerSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
			}
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].at < out[j].at })
		t.Logf("samples: %v", out)

		// look for two samples of the same series at the same timestamp with different values
		for i := 1; i < len(out); i++ {
			if out[i].at == out[i-1].at && out[i].value != out[i-1].value {
				t.Fatalf("two values at the same timestamp %v: %v and %v; the smaller one is dropped by deduplication: %v",
					out[i].at, out[i-1].value, out[i].value, out)
			}
		}
	})
}
