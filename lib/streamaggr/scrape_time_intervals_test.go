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

// One target's round 2 is processed after round 3 was written. It cannot be
// written at 2m, after 3m in the series: the counter would go back. It goes to
// 3m, off by the gap between the two targets rather than lost or misplaced.
func TestScrapeTimeIntervalsTotalNeverGoesBack(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples_total]\n  keep_metric_names: true\n" +
		"  staleness_interval: 40m\n  scrape_time_intervals: true\n"
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

func TestScrapeTimeIntervalsRefusesWhatItCannotDo(t *testing.T) {
	for _, cfg := range []string{
		"- match: foo\n  interval: 1m\n  outputs: [total]\n  scrape_time_intervals: true\n",
		"- match: foo\n  interval: 1m\n  outputs: [sum_samples, max]\n  scrape_time_intervals: true\n",
		"- match: foo\n  interval: 1m\n  dedup_interval: 30s\n  outputs: [sum_samples]\n  scrape_time_intervals: true\n",
		"- match: foo\n  interval: 1m\n  enable_windows: true\n  outputs: [sum_samples]\n  scrape_time_intervals: true\n",
		"- match: foo\n  interval: 1m\n  ignore_old_samples: true\n  outputs: [sum_samples]\n  scrape_time_intervals: true\n",
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
func TestScrapeTimeIntervalsGlobalOptionSkipsWhatItCannotApplyTo(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples_total]\n" +
		"- match: bar\n  interval: 1m\n  dedup_interval: 60s\n  outputs: [sum_samples]\n" +
		"- match: baz\n  interval: 1m\n  outputs: [max]\n"
	a, err := LoadFromData([]byte(cfg), func([]prompb.TimeSeries) {}, &Options{ScrapeTimeIntervals: true}, "test")
	if err != nil {
		t.Fatalf("the global option refused the config: %s", err)
	}
	defer a.MustStop()
	for i, want := range []bool{true, false, false} {
		if got := a.as[i].scrapeTimeIntervals; got != want {
			t.Errorf("aggregation %d: sample timestamps %v, want %v", i, got, want)
		}
	}
}

// A flush that runs a little after its boundary finds samples scraped after the
// boundary, of the interval still open. They must not be written into the
// interval that just closed, and not twice.
func TestScrapeTimeIntervalsOpenIntervalStaysOpen(t *testing.T) {
	for _, output := range []string{"sum_samples", "sum_samples_total"} {
		cfg := "- match: foo\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
			"  staleness_interval: 40m\n  scrape_time_intervals: true\n"
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

// The flush on shutdown writes the interval still open, before it ends, at the
// time of the shutdown. Right after a boundary that is still in the open
// interval, not in the one just closed.
func TestScrapeTimeIntervalsShutdownRightAfterBoundary(t *testing.T) {
	cfg := "- match: foo\n  interval: 1m\n  outputs: [sum_samples]\n  keep_metric_names: true\n" +
		"  flush_on_shutdown: true\n  scrape_time_intervals: true\n"
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

// A flush that takes longer than its interval reaches some entries only after
// the next interval has started, or ended: such an entry holds samples of the
// interval right after the boundary and of the one after that. Neither may be
// written ahead of the other, or sum_samples_total, which never writes a
// timestamp before one it has written, would move the first into the second.
func TestScrapeTimeIntervalsFlushLongerThanInterval(t *testing.T) {
	for _, output := range []string{"sum_samples", "sum_samples_total"} {
		cfg := "- match: '{__name__=~\"foo|bar\"}'\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
			"  staleness_interval: 40m\n  scrape_time_intervals: true\n"
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

// A round that arrives after a later interval was written goes to a later
// interval than its own, and every output of the aggregator has to put it in the
// same one. Here a free request in minute 3 moved the tokens on but not the cost,
// whose heartbeat is not due, so by its own latest point the cost of minute 1's
// late round would go back into minute 1 while its tokens went to minute 3: the
// two would be over in one minute and short in the other, and so in their hours.
func TestScrapeTimeIntervalsTotalLateRoundLandsTogether(t *testing.T) {
	cfg := "- match: '{__name__=~\"tokens|cost\"}'\n  interval: 1m\n  outputs: [sum_samples_total]\n  keep_metric_names: true\n" +
		"  staleness_interval: 40m\n  output_heartbeat_interval: 4m\n  scrape_time_intervals: true\n"
	pushes := []latePush{
		{at: 30 * time.Second, sampleAt: 30 * time.Second, metrics: "tokens 10\ncost 30"},
		{at: 150 * time.Second, sampleAt: 150 * time.Second, metrics: "tokens 5\ncost 0"}, // a free request
		{at: 190 * time.Second, sampleAt: 40 * time.Second, metrics: "tokens 1\ncost 3"},  // minute 1's, late
		{at: 250 * time.Second, sampleAt: 250 * time.Second, metrics: "tokens 2\ncost 6"}, // minute 5
	}
	got := runLateScenario(t, cfg, pushes, 8*time.Minute)
	// cost - 3*tokens of the requests in each minute: the free one's -15, and the
	// late round with it, whole
	for m, want := range map[int]float64{1: 0, 2: 0, 3: -15, 4: 0, 5: 0} {
		from, to := time.Duration(m-1)*time.Minute, time.Duration(m)*time.Minute
		tok, cost := increaseIn(got["tokens"], from, to), increaseIn(got["cost"], from, to)
		if d := cost - 3*tok; d != want {
			t.Errorf("minute %d: tokens %v, cost %v: cost - 3*tokens = %v, want %v; tokens %v, cost %v",
				m, tok, cost, d, want, got["tokens"], got["cost"])
		}
	}
	if tok, cost := increaseIn(got["tokens"], 0, 8*time.Minute), increaseIn(got["cost"], 0, 8*time.Minute); tok != 18 || cost != 39 {
		t.Errorf("in all: tokens %v, cost %v; want 18 and 39", tok, cost)
	}
}
