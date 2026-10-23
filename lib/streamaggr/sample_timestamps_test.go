package streamaggr

import (
	"fmt"
	"math/rand"
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

// Targets are scraped at offsets of their own, so the rounds of one minute reach
// an output with as many timestamps as there are targets. They are summed per
// minute and written once, at the end of the minute, not once per target; a
// sample right on a boundary belongs to the minute it ends.
func TestSampleTimestampsOneSamplePerIntervalAcrossTargets(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples]\n  keep_metric_names: true\n  sample_timestamps: true\n"
	var pushes []latePush
	// Three targets at 5s, 23s and 41s into minutes 1 to 3, all processed at
	// 4m30s, as while the aggregator is behind.
	for m := 1; m <= 3; m++ {
		for _, off := range []time.Duration{5 * time.Second, 23 * time.Second, 41 * time.Second} {
			pushes = append(pushes, latePush{at: 4*time.Minute + 30*time.Second, sampleAt: time.Duration(m)*time.Minute + off, metrics: `foo 1`})
		}
	}
	// A round scraped exactly at 5m belongs to (4m, 5m].
	pushes = append(pushes, latePush{at: 5*time.Minute + 10*time.Second, sampleAt: 5 * time.Minute, metrics: `foo 7`})
	got := runLateScenario(t, cfg, pushes, 7*time.Minute)["foo"]
	for m, want := range map[int]float64{1: 0, 2: 3, 3: 3, 4: 3, 5: 7, 6: 0} {
		to := time.Duration(m) * time.Minute
		if sum := sumIn(got, to-time.Minute, to); sum != want {
			t.Errorf("minute %d sums to %v, want %v; samples %v", m, sum, want, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("want one sample per minute with input, 4 in all; got %d: %v", len(got), got)
	}
}

// The totals of rounds from several targets are written once per minute, at the
// end of the minute, and the increase over every minute is the rounds in it.
func TestSampleTimestampsTotalOneSamplePerIntervalAcrossTargets(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples_total]\n  keep_metric_names: true\n" +
		"  staleness_interval: 40m\n  sample_timestamps: true\n"
	var pushes []latePush
	for m := 0; m < 3; m++ {
		for _, off := range []time.Duration{5 * time.Second, 23 * time.Second, 41 * time.Second} {
			pushes = append(pushes, latePush{at: 3*time.Minute + 30*time.Second, sampleAt: time.Duration(m)*time.Minute + off, metrics: `foo 1`})
		}
	}
	got := runLateScenario(t, cfg, pushes, 5*time.Minute)["foo"]
	for m := 1; m <= 3; m++ {
		to := time.Duration(m) * time.Minute
		if inc := increaseIn(got, to-time.Minute, to); inc != 3 {
			t.Errorf("increase over minute %d is %v, want 3; samples %v", m, inc, got)
		}
	}
	for _, s := range got {
		if s.at%time.Minute != 0 {
			t.Errorf("sample %v is not at the end of a minute; samples %v", s, got)
		}
	}
}

// The intervals samples are put into are the rule's own, on the grid its
// flushes are aligned to. time.Truncate counts from year 1, not from the Unix
// epoch, and for an interval that does not divide a day, such as 7m, the two
// grids differ: on the epoch's the sums would end between two flushes.
func TestSampleTimestampsIntervalsFollowTheFlushGrid(t *testing.T) {
	for _, iv := range []time.Duration{5 * time.Minute, 7 * time.Minute} {
		cfg := fmt.Sprintf("- match: foo\n  interval: %s\n  outputs: [sum_samples_total]\n  keep_metric_names: true\n"+
			"  staleness_interval: 2h\n  sample_timestamps: true\n", iv)
		var start time.Time
		synctest.Test(t, func(t *testing.T) { start = time.Now() })
		// The first interval on the flush grid that starts after the scenario does.
		first := start.Truncate(iv).Add(iv).Sub(start)
		var pushes []latePush
		// Samples at its start, inside it, at its very end and just after, all
		// processed one interval and a half late.
		for _, at := range []time.Duration{first + time.Millisecond, first + iv/2, first + iv, first + iv + time.Millisecond} {
			pushes = append(pushes, latePush{at: first + 3*iv, sampleAt: at, metrics: `foo 1`})
		}
		got := runLateScenario(t, cfg, pushes, first+5*iv)["foo"]
		for _, s := range got {
			if at := start.Add(s.at); !at.Equal(at.Truncate(iv)) {
				t.Errorf("interval %s: sample %v at %s is off the flush grid; samples %v", iv, s, at.Format("15:04:05.000"), got)
			}
		}
		if inc := increaseIn(got, first, first+iv); inc != 3 {
			t.Errorf("interval %s: increase over the first interval is %v, want 3; samples %v", iv, inc, got)
		}
		if inc := increaseIn(got, first+iv, first+2*iv); inc != 1 {
			t.Errorf("interval %s: increase over the second interval is %v, want 1; samples %v", iv, inc, got)
		}
	}
}

// A flush that runs a little after its boundary finds samples scraped after the
// boundary, of the interval still open. They must not be written into the
// interval that just closed, whatever the flush's back-off, and not twice.
func TestSampleTimestampsOpenIntervalStaysOpen(t *testing.T) {
	for _, output := range []string{"sum_samples", "sum_samples_total"} {
		cfg := "- match: foo\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
			"  staleness_interval: 40m\n  sample_timestamps: true\n"
		var got []outSample
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			var mu sync.Mutex
			pushFunc := func(tss []prompb.TimeSeries) {
				mu.Lock()
				defer mu.Unlock()
				for _, ts := range tss {
					for _, s := range ts.Samples {
						got = append(got, outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
					}
				}
			}
			a, err := LoadFromData([]byte(cfg), pushFunc, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			aggr := a.as[0]
			// Many flushes in, so the back-off is several hundred milliseconds.
			aggr.flushSeq.Store(600)
			T := start.Add(time.Minute)
			time.Sleep(T.Add(-10 * time.Second).Sub(time.Now()))
			a.Push(prometheus.MustParsePromMetrics(`foo 1`, T.Add(-10*time.Second).UnixMilli()), nil)
			// The flush for T runs 20ms late, and a round scraped 5ms after T is
			// already in.
			time.Sleep(T.Add(5 * time.Millisecond).Sub(time.Now()))
			a.Push(prometheus.MustParsePromMetrics(`foo 10`, T.Add(5*time.Millisecond).UnixMilli()), nil)
			time.Sleep(T.Add(20 * time.Millisecond).Sub(time.Now()))
			aggr.flush(pushFunc, T, aggr.cs.Load(), false)
			time.Sleep(start.Add(3 * time.Minute).Sub(time.Now()))
			a.MustStop()
		})
		sort.SliceStable(got, func(i, j int) bool { return got[i].at < got[j].at })
		first, second := sumIn(got, 0, time.Minute), sumIn(got, time.Minute, 2*time.Minute)
		if output == "sum_samples_total" {
			first, second = increaseIn(got, -time.Minute, time.Minute), increaseIn(got, time.Minute, 2*time.Minute)
		}
		if first != 1 || second != 10 {
			t.Errorf("%s: minute 1 got %v, want 1; minute 2 got %v, want 10; samples %v", output, first, second, got)
		}
		// Left for the next flush, the open interval is written at its end.
		for _, s := range got {
			if s.at > time.Minute && s.at < 2*time.Minute-time.Second {
				t.Errorf("%s: sample %v of minute 2 was written before minute 2 ended; samples %v", output, s, got)
			}
		}
	}
}

// The flush on shutdown writes the interval still open, before it ends. Right
// after a boundary, backing off must not put it into the interval just closed.
func TestSampleTimestampsShutdownRightAfterBoundary(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples]\n  keep_metric_names: true\n" +
		"  flush_on_shutdown: true\n  sample_timestamps: true\n"
	var got []outSample
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var mu sync.Mutex
		a, err := LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
			mu.Lock()
			defer mu.Unlock()
			for _, ts := range tss {
				for _, s := range ts.Samples {
					got = append(got, outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
				}
			}
		}, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		a.as[0].flushSeq.Store(600)
		T := start.Add(time.Minute)
		time.Sleep(T.Add(2 * time.Millisecond).Sub(time.Now()))
		a.Push(prometheus.MustParsePromMetrics(`foo 10`, T.Add(time.Millisecond).UnixMilli()), nil)
		time.Sleep(T.Add(5 * time.Millisecond).Sub(time.Now()))
		a.MustStop()
	})
	if sum := sumIn(got, time.Minute, 2*time.Minute); sum != 10 || len(got) != 1 {
		t.Fatalf("want 10 written once within minute 2; samples %v", got)
	}
}

// Every sample is counted exactly once, in the interval its timestamp falls in,
// however late the aggregator processes it and however long its flushes take:
// targets scraped at offsets of their own, rounds processed up to two and a
// half minutes late, flushes running over their interval, and a flush on
// shutdown. sum_samples has to match every interval exactly. sum_samples_total
// has to end at the sum of everything and never go back; an interval arriving
// after a later one was written goes to that later one by design.
func TestSampleTimestampsCountsEverySampleOnceInItsInterval(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		for _, output := range []string{"sum_samples", "sum_samples_total"} {
			for _, iv := range []time.Duration{time.Minute, 7 * time.Minute} {
				checkCountsEverySampleOnce(t, seed, output, iv)
			}
		}
	}
}

func checkCountsEverySampleOnce(t *testing.T, seed int64, output string, iv time.Duration) {
	t.Helper()
	cfg := fmt.Sprintf("- match: foo\n  interval: %s\n  outputs: [%s]\n  keep_metric_names: true\n"+
		"  staleness_interval: 2h\n  flush_on_shutdown: true\n  sample_timestamps: true\n", iv, output)
	rnd := rand.New(rand.NewSource(seed))
	type push struct {
		at, sampleAt time.Duration
		value        float64
	}
	const minutes = 40
	var pushes []push
	offsets := []time.Duration{0, 1 * time.Millisecond, 13 * time.Second, 37 * time.Second, 59*time.Second + 999*time.Millisecond}
	for m := 1; m < minutes; m++ {
		for _, off := range offsets {
			sampleAt := time.Duration(m)*time.Minute + off
			delay := time.Duration(rnd.Intn(2000)) * time.Millisecond
			if rnd.Intn(5) == 0 {
				delay = time.Duration(rnd.Intn(150)) * time.Second
			}
			pushes = append(pushes, push{at: sampleAt + delay, sampleAt: sampleAt, value: float64(1 + rnd.Intn(9))})
		}
	}
	sort.SliceStable(pushes, func(i, j int) bool { return pushes[i].at < pushes[j].at })
	flushDelays := make([]time.Duration, 100)
	for i := range flushDelays {
		if rnd.Intn(4) == 0 {
			flushDelays[i] = time.Duration(1+rnd.Intn(100)) * time.Second
		}
	}

	var got []outSample
	var start time.Time
	synctest.Test(t, func(t *testing.T) {
		start = time.Now()
		var mu sync.Mutex
		flushes := 0
		pushFunc := func(tss []prompb.TimeSeries) {
			mu.Lock()
			for _, ts := range tss {
				for _, s := range ts.Samples {
					got = append(got, outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
				}
			}
			d := flushDelays[flushes%len(flushDelays)]
			flushes++
			mu.Unlock()
			time.Sleep(d)
		}
		a, err := LoadFromData([]byte(cfg), pushFunc, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		for _, p := range pushes {
			time.Sleep(start.Add(p.at).Sub(time.Now()))
			a.Push(prometheus.MustParsePromMetrics(fmt.Sprintf("foo %v", p.value), start.Add(p.sampleAt).UnixMilli()), nil)
		}
		time.Sleep(start.Add(minutes*time.Minute + 5*time.Minute).Sub(time.Now()))
		a.MustStop()
	})
	sort.SliceStable(got, func(i, j int) bool { return got[i].at < got[j].at })

	// The true sums per interval, on the flushes' grid.
	end := func(at time.Duration) time.Duration {
		abs := start.Add(at)
		e := abs.Truncate(iv)
		if !e.Equal(abs) {
			e = e.Add(iv)
		}
		return e.Sub(start)
	}
	want := map[time.Duration]float64{}
	var all float64
	for _, p := range pushes {
		want[end(p.sampleAt)] += p.value
		all += p.value
	}
	name := fmt.Sprintf("seed=%d output=%s interval=%s", seed, output, iv)
	for _, s := range got {
		if s.at > minutes*time.Minute+5*time.Minute {
			t.Fatalf("%s: sample %v is after the end of the run", name, s)
		}
	}
	if output == "sum_samples" {
		for e, w := range want {
			if g := sumIn(got, e-iv, e); g != w {
				t.Errorf("%s: interval ending %s sums to %v, want %v", name, e, g, w)
			}
		}
		if g := sumIn(got, -time.Hour, 2*time.Hour); g != all {
			t.Errorf("%s: everything sums to %v, want %v", name, g, all)
		}
		return
	}
	for i := 1; i < len(got); i++ {
		if got[i].value < got[i-1].value {
			t.Fatalf("%s: the counter goes back from %v to %v", name, got[i-1], got[i])
		}
	}
	if inc := increaseIn(got, -time.Hour, 2*time.Hour); inc != all {
		t.Errorf("%s: the counter ends at %v, want %v", name, inc, all)
	}
}

// A flush that takes longer than its interval reaches some entries only after
// the next interval has started, or ended: such an entry holds samples of the
// interval right after the boundary and of the one after that. Neither may be
// written ahead of the other, or sum_samples_total, which never writes a
// timestamp before one it has written, would move the first into the second.
func TestSampleTimestampsFlushLongerThanInterval(t *testing.T) {
	for _, output := range []string{"sum_samples", "sum_samples_total"} {
		cfg := "- match: '{__name__=~\"foo|bar\"}'\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
			"  staleness_interval: 40m\n  sample_timestamps: true\n"
		var got []outSample
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			F := start.Add(time.Minute)
			var mu sync.Mutex
			pushFunc := func(tss []prompb.TimeSeries) {
				stuck := false
				mu.Lock()
				for _, ts := range tss {
					if ts.Labels[0].Value == "bar" || (len(ts.Labels) > 1 && ts.Labels[1].Value == "bar") {
						stuck = true
						continue
					}
					for _, s := range ts.Samples {
						got = append(got, outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
					}
				}
				mu.Unlock()
				if stuck && time.Now().Before(F.Add(time.Second)) {
					// The background flush at F is stuck for 200 seconds, as
					// a flush walking a million entries would be.
					time.Sleep(200 * time.Second)
				}
			}
			a, err := LoadFromData([]byte(cfg), pushFunc, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			aggr := a.as[0]
			time.Sleep(F.Add(-30 * time.Second).Sub(time.Now()))
			a.Push(prometheus.MustParsePromMetrics(`bar 1`, F.Add(-30*time.Second).UnixMilli()), nil)
			at := func(d time.Duration) { time.Sleep(F.Add(d).Sub(time.Now())) }
			at(30 * time.Second)
			a.Push(prometheus.MustParsePromMetrics(`foo 1`, F.Add(30*time.Second).UnixMilli()), nil)
			at(70 * time.Second)
			a.Push(prometheus.MustParsePromMetrics(`foo 10`, F.Add(70*time.Second).UnixMilli()), nil)
			// The stuck flush for F reaches foo's entry 90 seconds late.
			at(90 * time.Second)
			aggr.flush(pushFunc, F, aggr.cs.Load(), false)
			// The next flush to run is for F+2m.
			at(120 * time.Second)
			aggr.flush(pushFunc, F.Add(2*time.Minute), aggr.cs.Load(), false)
			at(10 * time.Minute)
			a.MustStop()
		})
		sort.SliceStable(got, func(i, j int) bool { return got[i].at < got[j].at })
		var m2, m3 float64
		if output == "sum_samples" {
			m2, m3 = sumIn(got, time.Minute, 2*time.Minute), sumIn(got, 2*time.Minute, 3*time.Minute)
		} else {
			m2, m3 = increaseIn(got, time.Minute, 2*time.Minute), increaseIn(got, 2*time.Minute, 3*time.Minute)
		}
		if m2 != 1 || m3 != 10 {
			t.Errorf("%s: minute 2 got %v, want 1; minute 3 got %v, want 10; samples %v", output, m2, m3, got)
		}
	}
}

// An entry that receives nothing for staleness_interval is dropped, but not
// while it holds sums of intervals no flush has written yet: a sample
// timestamped ahead of the clock waits for its interval to end, and the entry
// with it, on shutdown too.
func TestSampleTimestampsStaleEntryKeepsPendingSums(t *testing.T) {
	for _, stopAt := range []time.Duration{10 * time.Minute, 2*time.Minute + 30*time.Second} {
		cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples]\n  keep_metric_names: true\n" +
			"  staleness_interval: 1m\n  flush_on_shutdown: true\n  sample_timestamps: true\n"
		got := runLateScenario(t, cfg, []latePush{{at: 10 * time.Second, sampleAt: 2*time.Minute + 30*time.Second, metrics: `foo 5`}}, stopAt)["foo"]
		if sum := sumIn(got, 2*time.Minute, 3*time.Minute); sum != 5 || len(got) != 1 {
			t.Errorf("stopped at %s: want 5 written once within minute 3; samples %v", stopAt, got)
		}
	}
}
