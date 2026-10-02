package streamaggr

import (
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
)

// latePush is a push made at wall time at of samples scraped at sampleAt, both
// relative to the start; an aggregator that has fallen behind makes them apart.
type latePush struct {
	at, sampleAt time.Duration
	metrics      string
}

type outSample struct {
	at    time.Duration // the output timestamp relative to the start
	value float64
}

// runLateScenario feeds pushes to the aggregators in config under a fake clock
// that starts on a minute and returns their output samples by metric name, in
// timestamp order.
func runLateScenario(t *testing.T, config string, pushes []latePush, until time.Duration) map[string][]outSample {
	t.Helper()
	out := map[string][]outSample{}
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
			a.Push(prometheus.MustParsePromMetrics(p.metrics, start.Add(p.sampleAt).UnixMilli()), nil)
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
				out[name] = append(out[name], outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
			}
		}
		for name := range out {
			sort.SliceStable(out[name], func(i, j int) bool { return out[name][i].at < out[name][j].at })
		}
	})
	return out
}

// sumIn adds up the samples within (from, to], as sum_over_time() does after
// VM's deduplication has kept the biggest of the samples sharing a timestamp.
func sumIn(samples []outSample, from, to time.Duration) float64 {
	kept := map[time.Duration]float64{}
	for _, s := range samples {
		if v, ok := kept[s.at]; s.at > from && s.at <= to && (!ok || s.value > v) {
			kept[s.at] = s.value
		}
	}
	var sum float64
	for _, v := range kept {
		sum += v
	}
	return sum
}

// increaseIn is the increase of a counter over (from, to]: the last value at or
// before to minus the last value at or before from. VM keeps the biggest of
// samples sharing a timestamp, and so does this.
func increaseIn(samples []outSample, from, to time.Duration) float64 {
	last := func(at time.Duration) float64 {
		var v float64
		lastAt, found := time.Duration(0), false
		for _, s := range samples {
			if s.at > at {
				break
			}
			if !found || s.at > lastAt || s.value > v {
				v, lastAt, found = s.value, s.at, true
			}
		}
		return v
	}
	return last(to) - last(from)
}

// Rounds the aggregator processes four minutes late still count towards the
// minutes they were scraped in, and a sum arriving for a minute already written
// adds to it instead of replacing it.
func TestSampleTimestampsSumSamplesKeepsTheScrapeMinute(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples]\n  keep_metric_names: true\n  sample_timestamps: true\n"
	pushes := []latePush{
		// Rounds scraped at minutes 1 to 4, all processed at 5m10s.
		{at: 5*time.Minute + 10*time.Second, sampleAt: 1 * time.Minute, metrics: `foo 1`},
		{at: 5*time.Minute + 10*time.Second, sampleAt: 2 * time.Minute, metrics: `foo 2`},
		{at: 5*time.Minute + 10*time.Second, sampleAt: 3 * time.Minute, metrics: `foo 3`},
		{at: 5*time.Minute + 10*time.Second, sampleAt: 4 * time.Minute, metrics: `foo 4`},
		// Another target's share of minute 2 arrives after minute 2 was written.
		{at: 6*time.Minute + 10*time.Second, sampleAt: 2 * time.Minute, metrics: `foo 20`},
	}
	got := runLateScenario(t, cfg, pushes, 8*time.Minute)["foo"]
	for m, want := range map[int]float64{1: 1, 2: 22, 3: 3, 4: 4, 5: 0, 6: 0} {
		to := time.Duration(m) * time.Minute
		if sum := sumIn(got, to-time.Minute, to); sum != want {
			t.Errorf("minute %d sums to %v, want %v; samples %v", m, sum, want, got)
		}
	}
}

// Eleven rounds processed at once, eleven minutes late, land each in its own
// minute: the increase over any window is exactly the rounds scraped in it, and
// the counter never goes back. A heartbeat due while the aggregator was stuck
// does not push the series ahead of the rounds still to come.
func TestSampleTimestampsTotalAttributesLateRoundsToTheirMinute(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples_total]\n  keep_metric_names: true\n" +
		"  staleness_interval: 40m\n  output_heartbeat_interval: 4m\n  reset_marker_on_stale: true\n  sample_timestamps: true\n"
	pushes := []latePush{{at: 30 * time.Second, sampleAt: 0, metrics: `foo 1`}}
	for i := 1; i <= 11; i++ {
		pushes = append(pushes, latePush{at: 12*time.Minute + 5*time.Second, sampleAt: time.Duration(i) * time.Minute, metrics: `foo 1`})
	}
	got := runLateScenario(t, cfg, pushes, 14*time.Minute)["foo"]
	for i := 1; i < len(got); i++ {
		if got[i].value < got[i-1].value && got[i].value != 0 {
			t.Fatalf("the counter goes back from %v to %v: %v", got[i-1], got[i], got)
		}
	}
	if inc := increaseIn(got, 0, 3*time.Minute); inc != 3 {
		t.Errorf("increase over (0, 3m] is %v, want 3 (rounds 1-3); samples %v", inc, got)
	}
	if inc := increaseIn(got, 0, 5*time.Minute); inc != 5 {
		t.Errorf("increase over (0, 5m] is %v, want 5 (rounds 1-5); samples %v", inc, got)
	}
	if inc := increaseIn(got, 5*time.Minute, 11*time.Minute); inc != 6 {
		t.Errorf("increase over (5m, 11m] is %v, want 6 (rounds 6-11); samples %v", inc, got)
	}
}

// One target's round 2 is processed after round 3 was written. It cannot be
// written at 2m, after 3m in the series: the counter would go back. It goes to
// 3m, off by the gap between the two targets rather than lost or misplaced.
func TestSampleTimestampsTotalNeverGoesBack(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples_total]\n  keep_metric_names: true\n" +
		"  staleness_interval: 40m\n  sample_timestamps: true\n"
	pushes := []latePush{
		{at: 3*time.Minute + 10*time.Second, sampleAt: 3 * time.Minute, metrics: `foo 1`},
		{at: 4*time.Minute + 30*time.Second, sampleAt: 2 * time.Minute, metrics: `foo 1`},
	}
	got := runLateScenario(t, cfg, pushes, 6*time.Minute)["foo"]
	for i := 1; i < len(got); i++ {
		if got[i].value < got[i-1].value {
			t.Fatalf("the counter goes back from %v to %v: %v", got[i-1], got[i], got)
		}
	}
	if inc := increaseIn(got, 0, 6*time.Minute); inc != 2 {
		t.Fatalf("increase is %v, want 2; samples %v", inc, got)
	}
}

// The zero written before the first sample of a new output precedes it even
// when that sample is minutes older than the flush that writes it; a zero after
// it would read as a reset and count the value twice.
func TestSampleTimestampsResetMarkerPrecedesTheFirstSample(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples_total]\n  keep_metric_names: true\n" +
		"  staleness_interval: 40m\n  reset_marker_on_stale: true\n  sample_timestamps: true\n"
	got := runLateScenario(t, cfg, []latePush{{at: 7*time.Minute + 5*time.Second, sampleAt: 2 * time.Minute, metrics: `foo 5`}}, 9*time.Minute)["foo"]
	if len(got) < 2 || got[0].value != 0 || got[1].value != 5 || got[0].at >= got[1].at || got[1].at != 2*time.Minute {
		t.Fatalf("want a zero, then 5 at 2m; got %v", got)
	}
	if inc := increaseIn(got, 0, 9*time.Minute); inc != 5 {
		t.Fatalf("increase is %v, want 5; samples %v", inc, got)
	}
}

func TestSampleTimestampsRefusesWhatItCannotDo(t *testing.T) {
	for _, cfg := range []string{
		"- match: foo\n  interval: 1m\n  outputs: [total]\n  sample_timestamps: true\n",
		"- match: foo\n  interval: 1m\n  outputs: [sum_samples, max]\n  sample_timestamps: true\n",
		"- match: foo\n  interval: 1m\n  dedup_interval: 30s\n  outputs: [sum_samples]\n  sample_timestamps: true\n",
		"- match: foo\n  interval: 1m\n  enable_windows: true\n  outputs: [sum_samples]\n  sample_timestamps: true\n",
	} {
		a, err := LoadFromData([]byte(cfg), func([]prompb.TimeSeries) {}, nil, "test")
		if err == nil {
			a.MustStop()
			t.Errorf("accepted %q", strings.ReplaceAll(cfg, "\n", " "))
		}
	}
}

// The global option is one flag over a whole config: it leaves alone the
// aggregations it cannot apply to, such as a gauge de-duplicated per interval,
// and takes the rest.
func TestSampleTimestampsGlobalOptionSkipsWhatItCannotApplyTo(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples_total]\n" +
		"- match: bar\n  interval: 1m\n  dedup_interval: 60s\n  outputs: [sum_samples]\n" +
		"- match: baz\n  interval: 1m\n  outputs: [max]\n"
	a, err := LoadFromData([]byte(cfg), func([]prompb.TimeSeries) {}, &Options{SampleTimestamps: true}, "test")
	if err != nil {
		t.Fatalf("the global option refused the config: %s", err)
	}
	defer a.MustStop()
	for i, want := range []bool{true, false, false} {
		if got := a.as[i].sampleTimestamps; got != want {
			t.Errorf("aggregation %d: sample timestamps %v, want %v", i, got, want)
		}
	}
}
