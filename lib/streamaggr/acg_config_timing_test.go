package streamaggr

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// The push path measured against the shape ACG actually deploys: the labels the
// aggregation must not group by are removed with drop_input_labels rather than
// listed in without, and neither by nor without is set.
//
// That reaches a different branch from BenchmarkAggregatorsPush, which sets
// by: [job] and so sends ten of its twelve labels into the input part of the
// key. With neither by nor without, aggregateOnlyByTime is on and every
// remaining label goes to the output part, so the input part is empty.
//
// Worth knowing when reading the numbers on the commit that stopped building
// the input part unless an output reads it: that measured 119.9ms against
// 69.7ms on the by: [job] shape, and on this one the difference is inside the
// noise -- runs of one build spread over 92ms to 107ms. There is nothing for it
// to save here, because there was nothing in the input part to begin with.
// The change is still worth having for the configurations that do use by or
// without; it is simply not where this deployment's time goes.
func BenchmarkAggregatorsPushACGShape(b *testing.B) {
	for _, output := range []string{"sum_samples_total", "sum_samples"} {
		b.Run("output="+output, func(b *testing.B) {
			config := fmt.Sprintf(`
- match: http_requests_total
  interval: 24h
  drop_input_labels: [pod, instance, node]
  outputs: [%s]
  keep_metric_names: true
`, output)
			a, err := LoadFromData([]byte(config), func(_ []prompb.TimeSeries) {}, nil, "acg")
			if err != nil {
				b.Fatalf("cannot load config: %s", err)
			}
			defer a.MustStop()

			const loops = 100
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(len(benchSeries) * loops))
			b.RunParallel(func(pb *testing.PB) {
				var matchIdxs []byte
				for pb.Next() {
					for i := 0; i < loops; i++ {
						matchIdxs = a.Push(benchSeries, matchIdxs)
					}
				}
			})
		})
	}
}
