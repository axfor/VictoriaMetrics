// Copyright 2026 The VictoriaMetrics Authors
// Licensed under the Apache License, Version 2.0

package streamaggr

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promrelabel"
)

// The aggregation config recommended for the delta SDK selects on a label
// carrying the metric's type, so that a metric added later needs no change to
// the config. That rests on one selector detail: a matcher reads an absent label
// as empty, so a negative match on the type also takes every sample that carries
// no type at all -- the plain endpoints on the same aggregator, and every other
// target it scrapes -- which sum_samples_total would then add up as if each
// cumulative value were an increment. Pin it, so the config keeps the emptiness
// condition that stops that.
func TestTypeLabelSelectorTakesOnlyTypedSamples(t *testing.T) {
	ls := func(pairs ...string) []prompb.Label {
		out := make([]prompb.Label, 0, len(pairs)/2)
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, prompb.Label{Name: pairs[i], Value: pairs[i+1]})
		}
		return out
	}
	counter := ls("__name__", "m", "_metric_type", "counter")
	gauge := ls("__name__", "m", "_metric_type", "gauge")
	untyped := ls("__name__", "m")

	for _, tc := range []struct {
		selector                       string
		wantCounter, wantGauge, wantUn bool
	}{
		// What the config must not be: it takes the untyped sample too.
		{`{_metric_type!="gauge"}`, true, false, true},
		// What the config is.
		{`{_metric_type!="gauge",_metric_type!=""}`, true, false, false},
		{`{_metric_type="gauge"}`, false, true, false},
	} {
		var ie promrelabel.IfExpression
		if err := ie.Parse(tc.selector); err != nil {
			t.Fatalf("%s: %v", tc.selector, err)
		}
		if got := ie.Match(counter); got != tc.wantCounter {
			t.Errorf("%s on a counter: %v, want %v", tc.selector, got, tc.wantCounter)
		}
		if got := ie.Match(gauge); got != tc.wantGauge {
			t.Errorf("%s on a gauge: %v, want %v", tc.selector, got, tc.wantGauge)
		}
		if got := ie.Match(untyped); got != tc.wantUn {
			t.Errorf("%s on a sample with no type label: %v, want %v", tc.selector, got, tc.wantUn)
		}
	}
}
