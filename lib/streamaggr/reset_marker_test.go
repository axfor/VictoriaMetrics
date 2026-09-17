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

type pushAt struct {
	at      time.Duration
	metrics string
}

type markerSample struct {
	at    time.Duration // offset from the start of the scenario
	value float64
}

// runStaleScenario pushes the given metrics at the given offsets on a fake clock and returns output samples per metric name,
// ordered by timestamp.
//
// How many flushes an alive period produces depends on goroutine scheduling around interval boundaries,
// so the assertions below check the shape of the output instead of exact sample counts.
// Samples at or after settledBefore are dropped when settledBefore > 0, since the final flush at shutdown isn't deterministic.
func runStaleScenario(t *testing.T, config string, pushes []pushAt, until, settledBefore time.Duration) map[string][]markerSample {
	t.Helper()
	out := map[string][]markerSample{}
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var tss []prompb.TimeSeries
		pushFunc := func(src []prompb.TimeSeries) {
			mu.Lock()
			tss = appendClonedTimeseries(tss, src)
			mu.Unlock()
		}
		a, err := LoadFromData([]byte(config), pushFunc, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		start := time.Now()
		for _, p := range pushes {
			time.Sleep(start.Add(p.at).Sub(time.Now()))
			a.Push(prometheus.MustParsePromMetrics(p.metrics, time.Now().UnixMilli()), nil)
		}
		time.Sleep(start.Add(until).Sub(time.Now()))
		a.MustStop()

		mu.Lock()
		defer mu.Unlock()
		for _, ts := range tss {
			name := ""
			for _, l := range ts.Labels {
				if l.Name == "__name__" {
					name = l.Value
				}
			}
			for _, s := range ts.Samples {
				at := time.UnixMilli(s.Timestamp).Sub(start)
				if settledBefore > 0 && at >= settledBefore {
					continue
				}
				out[name] = append(out[name], markerSample{at, s.Value})
			}
		}
		for name := range out {
			sort.SliceStable(out[name], func(i, j int) bool { return out[name][i].at < out[name][j].at })
		}
	})
	return out
}

// collapse removes consecutive duplicate values.
func collapse(samples []markerSample) []float64 {
	var vs []float64
	for _, s := range samples {
		if n := len(vs); n == 0 || vs[n-1] != s.value {
			vs = append(vs, s.value)
		}
	}
	return vs
}

func equalFloats(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Two alive periods of the same counter with a gap longer than staleness_interval between them.
// The scrape target deleted the idle counter and recreated it from zero, so the second period reaches 5 again.
var twoPeriods = []pushAt{
	{0, `foo{pod="a"} 5`},
	{10 * time.Minute, `foo{pod="b"} 5`},
}

func markerConfig(outputs string, keepMetricNames, marker bool) string {
	s := `
- interval: 1m
  staleness_interval: 2m
  ignore_first_sample_interval: 0s
  without: [pod]
  outputs: ` + outputs + "\n"
	if keepMetricNames {
		s += "  keep_metric_names: true\n"
	}
	if marker {
		s += "  reset_marker_on_stale: true\n"
	}
	return s
}

// Without a marker the output is 5 → gap → 5, so increase() over the gap is 0 while the real increase is 5.
// With the marker a zero sample is emitted right after each alive period, which makes the reset explicit.
func TestResetMarkerOnStale(t *testing.T) {
	without := runStaleScenario(t, markerConfig("[total]", true, false), twoPeriods, 20*time.Minute, 18*time.Minute)["foo"]
	if got := collapse(without); !equalFloats(got, []float64{5}) {
		t.Fatalf("without marker the output must never drop to zero; got %v", without)
	}

	with := runStaleScenario(t, markerConfig("[total]", true, true), twoPeriods, 20*time.Minute, 18*time.Minute)["foo"]
	if got := collapse(with); !equalFloats(got, []float64{5, 0, 5, 0}) {
		t.Fatalf("with marker: got %v; want the shape [5 0 5 0]", with)
	}
	for i, s := range with {
		if s.value != 0 {
			continue
		}
		if i == 0 || with[i-1].value != 5 || s.at-with[i-1].at != time.Minute {
			t.Fatalf("a marker must follow the last flush of an alive period by one interval; got %v", with)
		}
		if i+1 < len(with) && with[i+1].at < 10*time.Minute-time.Minute {
			t.Fatalf("nothing must be emitted during the gap after a marker; got %v", with)
		}
	}
}

// The marker is emitted only for cumulative total outputs; increase output isn't affected.
//
// total and increase are flushed together, so within the same run increase must have exactly the flushes of total
// except the markers. (Comparing two separate runs isn't reliable: the number of flushes per alive period depends on scheduling.)
func TestResetMarkerOnStaleOnlyCumulativeTotal(t *testing.T) {
	const total, increase = "foo:1m_without_pod_total", "foo:1m_without_pod_increase"
	for range 5 {
		out := runStaleScenario(t, markerConfig("[total, increase]", false, true), twoPeriods, 20*time.Minute, 18*time.Minute)
		if got := collapse(out[total]); !equalFloats(got, []float64{5, 0, 5, 0}) {
			t.Fatalf("total output: got %v; want the shape [5 0 5 0]", out[total])
		}
		var totalFlushes []time.Duration
		for _, s := range out[total] {
			if s.value != 0 {
				totalFlushes = append(totalFlushes, s.at)
			}
		}
		if len(out[increase]) != len(totalFlushes) {
			t.Fatalf("increase output must have the flushes of total without markers; total %v; increase %v", out[total], out[increase])
		}
		for i, s := range out[increase] {
			if s.at != totalFlushes[i] {
				t.Fatalf("increase output must have the flushes of total without markers; total %v; increase %v", out[total], out[increase])
			}
		}
	}
}

// No marker is emitted at shutdown for series which are still alive.
// flush_on_shutdown is needed to observe the final flush at all: by default its output is dropped.
func TestResetMarkerOnStaleNotOnShutdown(t *testing.T) {
	config := markerConfig("[total]", true, true) + "  flush_on_shutdown: true\n"
	for range 5 {
		// Stop 90 seconds after the second push, while the series is still alive.
		alive := runStaleScenario(t, config, twoPeriods, 11*time.Minute+30*time.Second, 0)["foo"]
		if n := len(alive); n == 0 || alive[n-1].value != 5 {
			t.Fatalf("no marker is expected at shutdown for alive series; got %v", alive)
		}
	}
}

func TestResetMarkerOnStaleRequiresCumulativeTotal(t *testing.T) {
	for _, outputs := range []string{"[increase]", "[increase_prometheus]", "[sum_samples]"} {
		config := `
- interval: 1m
  outputs: ` + outputs + `
  reset_marker_on_stale: true
`
		if _, err := LoadFromData([]byte(config), func([]prompb.TimeSeries) {}, nil, "test"); err == nil {
			t.Fatalf("expecting an error for outputs %s", outputs)
		}
	}
	for _, outputs := range []string{"[total]", "[total_prometheus]", "[increase, total]"} {
		config := `
- interval: 1m
  outputs: ` + outputs + `
  reset_marker_on_stale: true
`
		a, err := LoadFromData([]byte(config), func([]prompb.TimeSeries) {}, nil, "test")
		if err != nil {
			t.Fatalf("unexpected error for outputs %s: %s", outputs, err)
		}
		a.MustStop()
	}
}
