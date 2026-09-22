package streamaggr

import (
	"testing"
	"time"
)

// Writing an output whose value has not moved is ingestion for a value the
// storage already holds. output_heartbeat_interval leaves those flushes out.
//
// It is only safe because a change is always written: a gap therefore spans an
// interval over which the value did not move, so the samples on either side of
// it are equal and increase() over any window containing the gap is unaffected.
//
// The scenario harness aligns pushes to flushes by wall clock, so which flush a
// push lands in varies between runs. These assert the invariants that do not --
// what gets left out, and that the total still arrives -- rather than a count.
func TestOutputHeartbeatLeavesOutRepeatsOnly(t *testing.T) {
	rule := func(hb string) string {
		return "- match: foo\n  interval: 1m\n  output_heartbeat_interval: " + hb +
			"\n  drop_input_labels: [pod]\n  outputs: [sum_samples_total]\n" +
			"  keep_metric_names: true\n  staleness_interval: 40m\n"
	}
	// Five increments of 1, one per interval, then two idle intervals.
	var pushes []pushAt
	for i := 0; i < 5; i++ {
		pushes = append(pushes, pushAt{at: time.Duration(i) * time.Minute, metrics: `foo{pod="a"} 1`})
	}
	const until = 7 * time.Minute

	values := func(hb string) []float64 {
		got := runStaleScenario(t, rule(hb), pushes, until, 0)["foo"]
		v := make([]float64, 0, len(got))
		for _, s := range got {
			v = append(v, s.value)
		}
		return v
	}

	off := values("0s")
	on := values("30m")

	if len(off) == 0 || len(on) == 0 {
		t.Fatalf("no samples: off=%v on=%v", off, on)
	}
	// The total has to survive whatever was skipped.
	if off[len(off)-1] != 5 {
		t.Fatalf("without the heartbeat the total is %v, want 5 -- the scenario itself is wrong", off[len(off)-1])
	}
	if last := on[len(on)-1]; last != 5 {
		t.Errorf("with the heartbeat the total is %v, want 5: an increment was lost with a skipped flush", last)
	}
	// Every value written has to be a change. Repeats are exactly what is left out.
	for i := 1; i < len(on); i++ {
		if on[i] == on[i-1] {
			t.Errorf("wrote %v twice in a row (%v): an unchanged flush was not skipped", on[i], on)
		}
	}
	// And the baseline has to contain a repeat, or this proves nothing.
	repeated := false
	for i := 1; i < len(off); i++ {
		if off[i] == off[i-1] {
			repeated = true
		}
	}
	if !repeated {
		t.Fatalf("the baseline %v has no repeated value, so there was nothing to skip -- the scenario is too short", off)
	}
	if len(on) >= len(off) {
		t.Errorf("heartbeat wrote %d samples, baseline %d: it should write fewer (%v vs %v)", len(on), len(off), on, off)
	}
}

// Off by default: an empty or zero interval writes every flush, as before.
func TestOutputHeartbeatIsOffByDefault(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  drop_input_labels: [pod]\n  outputs: [sum_samples_total]\n" +
		"  keep_metric_names: true\n  staleness_interval: 40m\n"
	got := runStaleScenario(t, cfg, []pushAt{{at: 0, metrics: `foo{pod="a"} 1`}}, 5*time.Minute, 0)["foo"]
	repeated := false
	for i := 1; i < len(got); i++ {
		if got[i].value == got[i-1].value {
			repeated = true
		}
	}
	if !repeated {
		t.Errorf("without output_heartbeat_interval every flush has to be written, but %d samples carry no repeat", len(got))
	}
}

// A heartbeat below the flush interval is a configuration error rather than a
// no-op: an output is only written on flush, so it could never take effect.
func TestOutputHeartbeatRejectsSubIntervalValues(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  output_heartbeat_interval: 10s\n  outputs: [sum_samples_total]\n"
	if _, err := LoadFromData([]byte(cfg), nil, &Options{}, "test"); err == nil {
		t.Error("a heartbeat shorter than the interval was accepted")
	}
}
