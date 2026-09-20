package streamaggr

import (
	"testing"
)

// The input part of a sample's key carries the labels the aggregation groups
// away, and it is left out when no configured output reads it. Getting that
// wrong does not fail: an output that keeps state per input series would see
// every input as the same one and quietly aggregate the wrong thing. So the
// answer each output gives has to match the pushSample it is paired with --
// the ones taking a named key parameter need it, the ones taking _ do not.
func TestOutputsDeclareWhetherTheyNeedTheInputKey(t *testing.T) {
	for _, tc := range []struct {
		output string
		want   bool
	}{
		{"count_series", true},
		{"increase", true},
		{"increase_prometheus", true},
		{"rate_avg", true},
		{"rate_sum", true},
		{"total", true},
		{"total_prometheus", true},

		{"avg", false},
		{"count_samples", false},
		{"histogram_bucket", false},
		{"last", false},
		{"max", false},
		{"min", false},
		{"quantiles(0.5)", false},
		{"stddev", false},
		{"stdvar", false},
		{"sum_samples", false},
		{"sum_samples_total", false},
		{"unique_samples", false},
	} {
		t.Run(tc.output, func(t *testing.T) {
			ac, err := newOutputConfig(tc.output, map[string]struct{}{}, false, 0)
			if err != nil {
				t.Fatalf("%s: %v", tc.output, err)
			}
			if got := ac.needsInputKey(); got != tc.want {
				t.Errorf("%s needsInputKey = %v, want %v", tc.output, got, tc.want)
			}
		})
	}
}

// And the aggregator has to take the union: one output that needs the input
// key is enough to keep it for the whole rule.
func TestInputKeyIsKeptWhenAnyOutputNeedsIt(t *testing.T) {
	for _, tc := range []struct {
		outputs string
		want    bool
	}{
		{"[sum_samples_total]", false},
		{"[sum_samples, last, max]", false},
		{"[total]", true},
		{"[sum_samples_total, count_series]", true},
		{"[count_series, sum_samples]", true},
	} {
		t.Run(tc.outputs, func(t *testing.T) {
			cfg := `
- match: '{__name__="m"}'
  interval: 10s
  without: [pod]
  outputs: ` + tc.outputs + `
`
			as, err := LoadFromData([]byte(cfg), nil, &Options{}, "test")
			if err != nil {
				t.Fatalf("cannot load config: %v", err)
			}
			defer as.MustStop()
			if got := as.as[0].aggrOutputs.useInputKey; got != tc.want {
				t.Errorf("outputs %s: useInputKey = %v, want %v", tc.outputs, got, tc.want)
			}
		})
	}
}
