package streamaggr

import (
	"testing"
	"time"
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

// Leaving the input part out of a sample's key is only safe when nothing reads
// it. With dedup_interval set, useInputKey is already false for a different
// reason -- the deduplicator keys its own map on the full key -- so reading
// that one flag to decide the key layout collapses every input series in an
// output group onto a single sample, and the output reads as if only one pod
// had reported.
func TestDedupKeepsTheInputPartOfTheKey(t *testing.T) {
	const threePods = `foo{pod="a"} 1
foo{pod="b"} 1
foo{pod="c"} 1`

	for _, tc := range []struct {
		name  string
		dedup string
	}{
		{"dedup off", ""},
		{"dedup on", "  dedup_interval: 30s\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := "- match: foo\n  interval: 1m\n" + tc.dedup + "  without: [pod]\n  outputs: [sum_samples]\n  keep_metric_names: true\n"
			got := runStaleScenario(t, cfg, []pushAt{{at: 0, metrics: threePods}}, 2*time.Minute, 0)["foo"]
			if len(got) == 0 {
				t.Fatalf("no output samples")
			}
			if got[0].value != 3 {
				t.Errorf("three pods each reporting 1 summed to %v, want 3 -- the input part of the key was dropped, so they shared one entry", got[0].value)
			}
		})
	}
}

// drop_input_labels removes a label before de-duplication, while without
// removes it at grouping time, after. For a rule that sums a gauge across the
// pods of one deployment, the difference decides the answer: listing the pod
// label under drop_input_labels leaves the pods indistinguishable to the
// deduplicator, which keeps one sample per series per interval, so the sum is
// taken over a single pod. It does not fail; the value is simply one pod's.
//
// This pins the shape ACG deploys (docs/apikey-usage/integration.md, section
// four), where the same rule needs both: dedup, so a pod scraped twice inside
// one interval is not counted twice, and the pod labels gone from the output,
// so the three higress pods land on one series.
func TestGaugeRuleSumsAcrossPodsWithDedupOn(t *testing.T) {
	const threePods = `foo{pod="a",instance="a",_metric_type="gauge"} 1
foo{pod="b",instance="b",_metric_type="gauge"} 1
foo{pod="c",instance="c",_metric_type="gauge"} 1`
	// pod a is scraped twice inside one interval: 5 then 7. Dedup has to keep
	// the later one rather than let sum_samples add both.
	const podAgain = `foo{pod="a",instance="a",_metric_type="gauge"} 7`
	const podFirst = `foo{pod="a",instance="a",_metric_type="gauge"} 5
foo{pod="b",instance="b",_metric_type="gauge"} 1
foo{pod="c",instance="c",_metric_type="gauge"} 1`

	const rule = `
- match: '{_metric_type="gauge"}'
  interval: 1m
  drop_input_labels: [_metric_type]
  without: [pod, instance, node]
  outputs: [sum_samples]
  keep_metric_names: true
  dedup_interval: 60s
`
	for _, tc := range []struct {
		name   string
		pushes []pushAt
		want   float64
	}{
		{"three pods each reporting 1", []pushAt{{at: 0, metrics: threePods}}, 3},
		{"one of them scraped twice", []pushAt{
			{at: 0, metrics: podFirst},
			{at: 10 * time.Second, metrics: podAgain},
		}, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runStaleScenario(t, rule, tc.pushes, 2*time.Minute, 0)["foo"]
			if len(got) == 0 {
				t.Fatalf("no output samples")
			}
			if got[0].value != tc.want {
				t.Errorf("sum is %v, want %v -- moving the pod label into "+
					"drop_input_labels makes the pods one series to the deduplicator",
					got[0].value, tc.want)
			}
		})
	}
}
