package streamaggr

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// The ACG shape under sample_timestamps: every output is fed by many targets,
// and vmagent scrapes each target at an offset of its own, so the rounds of one
// minute reach an output with as many distinct timestamps as there are targets.
// lag is how many minutes of rounds pile up between two flushes, as while
// vmagent is behind.
func BenchmarkSampleTimestampsManyTargets(b *testing.B) {
	const (
		targets = 20
		keys    = 2000
	)
	for _, lag := range []int{1, 4} {
		for _, mode := range []string{"off", "on"} {
			b.Run(fmt.Sprintf("lag=%dm/sample_timestamps=%s", lag, mode), func(b *testing.B) {
				config := `
- match: acg_cost_total
  interval: 1m
  by: [apikey_id]
  outputs: [sum_samples]
`
				if mode == "on" {
					config += "  sample_timestamps: true\n"
				}
				var outSamples atomic.Int64
				pushFunc := func(tss []prompb.TimeSeries) {
					n := 0
					for _, ts := range tss {
						n += len(ts.Samples)
					}
					outSamples.Add(int64(n))
				}
				a, err := LoadFromData([]byte(config), pushFunc, nil, "bench")
				if err != nil {
					b.Fatalf("cannot load config: %s", err)
				}
				defer a.MustStop()
				aggr := a.as[0]

				rounds := make([][]prompb.TimeSeries, targets)
				for t := range rounds {
					tss := make([]prompb.TimeSeries, keys)
					for k := range tss {
						tss[k] = prompb.TimeSeries{
							Labels: []prompb.Label{
								{Name: "__name__", Value: "acg_cost_total"},
								{Name: "apikey_id", Value: fmt.Sprintf("k%d", k)},
								{Name: "instance", Value: fmt.Sprintf("pod-%d", t)},
							},
							Samples: []prompb.Sample{{Value: 1}},
						}
					}
					rounds[t] = tss
				}

				minute := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
				// the rounds are on time: the interval before them is the one written last
				aggr.stWritten.Store(minute)
				var matchIdxs []byte
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for m := 0; m < lag; m++ {
						for t, tss := range rounds {
							// each target's own offset into the minute
							ts := minute + int64(t)*2_917
							for k := range tss {
								tss[k].Samples[0].Timestamp = ts
							}
							matchIdxs = a.Push(tss, matchIdxs)
						}
						minute += 60_000
					}
					aggr.flush(pushFunc, time.UnixMilli(minute), aggr.cs.Load(), false)
				}
				b.StopTimer()
				b.ReportMetric(float64(outSamples.Load())/float64(b.N*keys), "out-samples/series/flush")
			})
		}
	}
}
