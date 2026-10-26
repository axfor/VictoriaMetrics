package streamaggr

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
)

// The rules of scrape_time_intervals, as these tests check them. A sample belongs
// to the interval [end - interval, end) its scrape timestamp falls in, and is
// written at the end of it:
//
//   - processed before that interval is written, it is written with it;
//   - processed after, while it is still the interval written last, it is the
//     tail of a round that crossed the boundary: the interval is written again,
//     at the same timestamp, complete, and VM keeps the bigger value;
//   - processed later than that, it goes to the first interval not written.
//
// Either way every sample is counted once, and the samples of one round -- one
// push here -- go to the same interval in every output.

// tokensCost is a round of usage: tokens, and cost three times as much.
func tokensCost(v float64) string { return fmt.Sprintf("tokens %v\ncost %v", v, 3*v) }

func stConfig(output string, extra string) string {
	return "- match: '{__name__=~\"tokens|cost\"}'\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
		"  staleness_interval: 40m\n  scrape_time_intervals: true\n" + extra
}

// perInterval is what an output wrote for the interval ending at to: the sum
// of its samples for sum_samples, the increase for sum_samples_total.
func perInterval(output string, samples []outSample, to, iv time.Duration) float64 {
	if output == "sum_samples" {
		return sumIn(samples, to-iv, to)
	}
	return increaseIn(samples, to-iv, to)
}

// checkMinutes checks tokens per minute against want, and that cost is three
// times the tokens in every minute: the samples of a round were not split.
func checkMinutes(t *testing.T, output string, got map[string][]outSample, want map[int]float64, minutes int) {
	t.Helper()
	for m := 1; m <= minutes; m++ {
		to := time.Duration(m) * time.Minute
		tok, cost := perInterval(output, got["tokens"], to, time.Minute), perInterval(output, got["cost"], to, time.Minute)
		if tok != want[m] {
			t.Errorf("%s: minute ending %dm has tokens %v, want %v; tokens %v", output, m, tok, want[m], got["tokens"])
		}
		if cost != 3*tok {
			t.Errorf("%s: minute ending %dm has tokens %v but cost %v: a round was split; cost %v", output, m, tok, cost, got["cost"])
		}
	}
}

var stOutputs = []string{"sum_samples", "sum_samples_total"}

// Rounds processed within their interval are written with it, once per
// interval however many targets feed it.
func TestScrapeTimeIntervalsOnTimeRoundsGoToTheirInterval(t *testing.T) {
	for _, output := range stOutputs {
		var pushes []latePush
		for m := 0; m < 3; m++ {
			for _, off := range []time.Duration{5 * time.Second, 23 * time.Second, 41 * time.Second} {
				at := time.Duration(m)*time.Minute + off
				pushes = append(pushes, latePush{at: at + 2*time.Second, sampleAt: at, metrics: tokensCost(1)})
			}
		}
		got := runLateScenario(t, stConfig(output, ""), pushes, 5*time.Minute)
		checkMinutes(t, output, got, map[int]float64{1: 3, 2: 3, 3: 3}, 5)
		for _, s := range got["tokens"] {
			if s.at%time.Minute != 0 {
				t.Errorf("%s: sample %v is not at the end of a minute", output, s)
			}
		}
		if output == "sum_samples" && len(got["tokens"]) != 3 {
			t.Errorf("%s: want one sample per minute, 3 in all; got %v", output, got["tokens"])
		}
	}
}

// A round scraped right on a boundary, as scrape_align_interval has them,
// belongs to the interval that starts there; one scraped a millisecond before
// it, to the one that ends there.
func TestScrapeTimeIntervalsBoundaryBelongsToTheIntervalItStarts(t *testing.T) {
	for _, output := range stOutputs {
		got := runLateScenario(t, stConfig(output, ""), []latePush{
			{at: 2*time.Minute - time.Millisecond, sampleAt: 2*time.Minute - time.Millisecond, metrics: tokensCost(1)},
			{at: 2*time.Minute + 10*time.Second, sampleAt: 2 * time.Minute, metrics: tokensCost(5)},
		}, 4*time.Minute)
		checkMinutes(t, output, got, map[int]float64{2: 1, 3: 5}, 4)
	}
}

// The tail of a round that crossed the boundary -- processed after its interval
// was written, before the next one was -- completes that interval: it is written
// again at the same timestamp, with the whole round.
func TestScrapeTimeIntervalsTailCompletesTheIntervalWrittenLast(t *testing.T) {
	for _, output := range stOutputs {
		got := runLateScenario(t, stConfig(output, ""), []latePush{
			{at: 90 * time.Second, sampleAt: 80 * time.Second, metrics: tokensCost(4)},
			{at: 2*time.Minute + 10*time.Second, sampleAt: 80 * time.Second, metrics: tokensCost(6)},
		}, 4*time.Minute)
		checkMinutes(t, output, got, map[int]float64{2: 10, 3: 0}, 4)
		n := 0
		for _, s := range got["tokens"] {
			if s.at == 2*time.Minute {
				n++
			}
		}
		if n != 2 {
			t.Errorf("%s: want the minute ending 2m written twice, the second time complete; tokens %v", output, got["tokens"])
		}
	}
}

// A tail that would make the interval smaller cannot complete it -- VM keeps the
// bigger of two values at one timestamp -- so it goes to the next interval.
func TestScrapeTimeIntervalsNegativeTailGoesToTheNextInterval(t *testing.T) {
	got := runLateScenario(t, stConfig("sum_samples", ""), []latePush{
		{at: 90 * time.Second, sampleAt: 80 * time.Second, metrics: `tokens 4`},
		{at: 2*time.Minute + 10*time.Second, sampleAt: 80 * time.Second, metrics: `tokens -1`},
	}, 4*time.Minute)["tokens"]
	if a, b := sumIn(got, time.Minute, 2*time.Minute), sumIn(got, 2*time.Minute, 3*time.Minute); a != 4 || b != -1 {
		t.Fatalf("want 4 in the minute ending 2m and -1 in the next; got %v and %v: %v", a, b, got)
	}
}

// Rounds whose interval has ended by the time they are processed go to the
// interval they are processed in, all of them together. Nothing is lost,
// nothing counted twice.
func TestScrapeTimeIntervalsLateRoundsGoToTheFirstIntervalNotWritten(t *testing.T) {
	for _, output := range stOutputs {
		got := runLateScenario(t, stConfig(output, ""), []latePush{
			// scraped in the minutes ending 2m to 5m, all processed at 5m10s
			{at: 5*time.Minute + 10*time.Second, sampleAt: 1 * time.Minute, metrics: tokensCost(1)},
			{at: 5*time.Minute + 10*time.Second, sampleAt: 2 * time.Minute, metrics: tokensCost(2)},
			{at: 5*time.Minute + 10*time.Second, sampleAt: 3 * time.Minute, metrics: tokensCost(3)},
			{at: 5*time.Minute + 10*time.Second, sampleAt: 4 * time.Minute, metrics: tokensCost(4)},
			// another target's, scraped in the minute ending 3m, processed at 6m10s
			{at: 6*time.Minute + 10*time.Second, sampleAt: 2*time.Minute + 7*time.Second, metrics: tokensCost(20)},
		}, 8*time.Minute)
		checkMinutes(t, output, got, map[int]float64{6: 1 + 2 + 3 + 4, 7: 20}, 8)
	}
}

// The rest of a late round processed after the flush of the interval it was
// put in goes there too, as that interval's tail: samples sharing a scrape
// timestamp are taken as one round, whose interval is decided at the first.
func TestScrapeTimeIntervalsLateRoundKeepsItsInterval(t *testing.T) {
	for _, output := range stOutputs {
		got := runLateScenario(t, stConfig(output, ""), []latePush{
			{at: 5*time.Minute + 10*time.Second, sampleAt: 2 * time.Minute, metrics: tokensCost(2)},
			{at: 6*time.Minute + 10*time.Second, sampleAt: 2 * time.Minute, metrics: tokensCost(20)},
		}, 8*time.Minute)
		checkMinutes(t, output, got, map[int]float64{6: 22}, 8)
	}
}

// Rounds scraped before the aggregator started go to the first interval it
// writes: the one before is the previous process's, which may have written it.
func TestScrapeTimeIntervalsRoundsBeforeTheStartGoToTheFirstInterval(t *testing.T) {
	for _, output := range stOutputs {
		got := runLateScenario(t, stConfig(output, ""), []latePush{
			{at: 10 * time.Second, sampleAt: -30 * time.Second, metrics: tokensCost(7)},
		}, 3*time.Minute)
		checkMinutes(t, output, got, map[int]float64{1: 7}, 3)
		for _, s := range got["tokens"] {
			if s.at <= 0 && s.value != 0 {
				t.Errorf("%s: %v written at or before the start", output, s)
			}
		}
	}
}

// A sample stamped ahead of the clock counts as processed now; held until its
// time came, it would keep its output from being dropped as stale.
func TestScrapeTimeIntervalsFutureTimestampCountsNow(t *testing.T) {
	got := runLateScenario(t, stConfig("sum_samples", "  flush_on_shutdown: true\n"), []latePush{
		{at: 10 * time.Second, sampleAt: 2*time.Minute + 30*time.Second, metrics: `tokens 5`},
	}, 4*time.Minute)["tokens"]
	if sumIn(got, 0, time.Minute) != 5 || len(got) != 1 {
		t.Fatalf("want 5 written once in the minute ending 1m; got %v", got)
	}
}

// sum_samples_total never goes back, puts the reset marker before its first
// sample, and its heartbeat does not get ahead of the intervals written.
func TestScrapeTimeIntervalsTotalStaysACounter(t *testing.T) {
	cfg := stConfig("sum_samples_total", "  output_heartbeat_interval: 4m\n  reset_marker_on_stale: true\n")
	pushes := []latePush{{at: 30 * time.Second, sampleAt: 0, metrics: tokensCost(1)}}
	for i := 1; i <= 11; i++ {
		pushes = append(pushes, latePush{at: 12*time.Minute + 5*time.Second, sampleAt: time.Duration(i) * time.Minute, metrics: tokensCost(1)})
	}
	got := runLateScenario(t, cfg, pushes, 14*time.Minute)
	tok := got["tokens"]
	if len(tok) == 0 || tok[0].value != 0 {
		t.Fatalf("want the reset marker first; got %v", tok)
	}
	for i := 1; i < len(tok); i++ {
		if tok[i].value < tok[i-1].value {
			t.Fatalf("the counter goes back from %v to %v: %v", tok[i-1], tok[i], tok)
		}
	}
	// the eleven rounds' minutes have all ended: they go to the one they are processed in
	checkMinutes(t, "sum_samples_total", got, map[int]float64{1: 1, 13: 11}, 14)
}

// However far behind the aggregator is, an output holds only a few sums: the
// samples of intervals before the one written last go to the first one not
// written. What is left is that one, the tail of the one written last and, for
// sum_samples, what was written for it, and the next one.
func TestScrapeTimeIntervalsPendingStaysBounded(t *testing.T) {
	for _, output := range stOutputs {
		synctest.Test(t, func(t *testing.T) {
			a, err := LoadFromData([]byte(stConfig(output, "")), func([]prompb.TimeSeries) {}, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			defer a.MustStop()
			start := time.Now()
			time.Sleep(30*time.Minute + 10*time.Second)
			// a round of every minute so far, all processed now, and one of now
			for m := 0; m <= 30; m++ {
				a.Push(prometheus.MustParsePromMetrics(tokensCost(1), start.Add(time.Duration(m)*time.Minute+time.Second).UnixMilli()), nil)
			}
			for i := range a.as[0].aggrOutputs.shards {
				sh := &a.as[0].aggrOutputs.shards[i]
				sh.mu.RLock()
				for k, av := range sh.m {
					var p *bySampleTime
					switch v := av.one[0].(type) {
					case *sumSamplesAtValue:
						p = &v.pending
					case *sumSamplesTotalAtValue:
						p = &v.pending
					}
					if n := len(p.take(nil)); n > 4 {
						t.Errorf("%s: output %q holds %d sums", output, k, n)
					}
				}
				sh.mu.RUnlock()
			}
		})
	}
}

// Every sample is counted exactly once, a round's samples always go to the same
// interval, and each goes where the rules put it: rounds from targets scraped
// at offsets of their own, some right on a boundary, processed up to two and a
// half minutes late.
//
// Without flushes taking longer than their interval, where a sample goes follows
// from when it was processed alone, and every interval is checked against that.
// With them -- the flush holding up the next one -- it depends on what the flush
// had reached, and the intervals are checked for what holds regardless: a round
// never split, no sample written before its interval, and everything counted.
func TestScrapeTimeIntervalsRules(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		for _, output := range stOutputs {
			for _, iv := range []time.Duration{time.Minute, 7 * time.Minute} {
				for _, overrun := range []bool{false, true} {
					checkScrapeTimeIntervalsRules(t, seed, output, iv, overrun)
				}
			}
		}
	}
}

func checkScrapeTimeIntervalsRules(t *testing.T, seed int64, output string, iv time.Duration, overrun bool) {
	t.Helper()
	name := fmt.Sprintf("seed=%d output=%s interval=%s overrun=%v", seed, output, iv, overrun)
	cfg := fmt.Sprintf("- match: '{__name__=~\"tokens|cost\"}'\n  interval: %s\n  outputs: [%s]\n  keep_metric_names: true\n"+
		"  staleness_interval: 2h\n  flush_on_shutdown: true\n  scrape_time_intervals: true\n", iv, output)
	rnd := rand.New(rand.NewSource(seed))
	type push struct {
		at, sampleAt time.Duration
		value        float64
	}
	const minutes = 40
	var pushes []push
	offsets := []time.Duration{0, time.Millisecond, 13 * time.Second, 37 * time.Second, 59*time.Second + 999*time.Millisecond}
	for m := 1; m < minutes; m++ {
		for _, off := range offsets {
			sampleAt := time.Duration(m)*time.Minute + off
			delay := time.Duration(1+rnd.Intn(2000)) * time.Millisecond
			if rnd.Intn(5) == 0 {
				delay = time.Duration(rnd.Intn(150))*time.Second + time.Duration(1+rnd.Intn(999))*time.Millisecond
			}
			pushes = append(pushes, push{at: sampleAt + delay, sampleAt: sampleAt, value: float64(1 + rnd.Intn(9))})
		}
	}
	sort.SliceStable(pushes, func(i, j int) bool { return pushes[i].at < pushes[j].at })
	flushDelays := make([]time.Duration, 100)
	for i := range flushDelays {
		if overrun && rnd.Intn(4) == 0 {
			flushDelays[i] = time.Duration(1+rnd.Intn(100)) * time.Second
		}
	}

	got := map[string][]outSample{}
	var start time.Time
	var phase int64
	synctest.Test(t, func(t *testing.T) {
		start = time.Now()
		var mu sync.Mutex
		flushes := 0
		pushFunc := func(tss []prompb.TimeSeries) {
			mu.Lock()
			for _, ts := range tss {
				name := ""
				for _, l := range ts.Labels {
					if l.Name == "__name__" {
						name = l.Value
					}
				}
				for _, s := range ts.Samples {
					got[name] = append(got[name], outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
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
		phase = a.as[0].intervalPhaseMsec
		for _, p := range pushes {
			time.Sleep(start.Add(p.at).Sub(time.Now()))
			a.Push(prometheus.MustParsePromMetrics(tokensCost(p.value), start.Add(p.sampleAt).UnixMilli()), nil)
		}
		time.Sleep(start.Add(minutes*time.Minute + 5*time.Minute).Sub(time.Now()))
		a.MustStop()
	})
	for k := range got {
		sort.SliceStable(got[k], func(i, j int) bool { return got[k][i].at < got[k][j].at })
	}

	// The flushes' grid, relative to the start: floor gives the boundary at or
	// before at, end the end of the interval [end - iv, end) at falls in.
	ivMs := iv.Milliseconds()
	floor := func(at time.Duration) time.Duration {
		abs := start.Add(at).UnixMilli()
		d := abs - phase
		q := d / ivMs
		if d < 0 && d%ivMs != 0 {
			q--
		}
		return time.UnixMilli(phase + q*ivMs).Sub(start)
	}
	end := func(at time.Duration) time.Duration { return floor(at) + iv }

	var all float64
	own := map[time.Duration]float64{} // by the interval each was scraped in
	for _, p := range pushes {
		all += p.value
		own[end(p.sampleAt)] += p.value
	}
	ends := map[time.Duration]bool{}
	for e := range own {
		ends[e], ends[e+iv] = true, true
	}
	// the flush on shutdown waits for the one before, which a flush delay holds up
	runEnd := minutes*time.Minute + 5*time.Minute
	for _, d := range flushDelays {
		runEnd = max(runEnd, minutes*time.Minute+5*time.Minute+d)
	}
	for _, s := range got["tokens"] {
		ends[s.at] = true
		if s.at > runEnd {
			t.Fatalf("%s: sample %v is after the end of the run", name, s)
		}
	}

	// Everything counted once, and a round never split: cost is three times the
	// tokens over everything and in every interval.
	total := func(samples []outSample) float64 {
		if output == "sum_samples" {
			return sumIn(samples, -time.Hour, 2*time.Hour)
		}
		return increaseIn(samples, -time.Hour, 2*time.Hour)
	}
	if tok, cost := total(got["tokens"]), total(got["cost"]); tok != all || cost != 3*all {
		t.Errorf("%s: tokens %v, cost %v; want %v and %v", name, tok, cost, all, 3*all)
	}
	for e := range ends {
		tok, cost := perInterval(output, got["tokens"], e, iv), perInterval(output, got["cost"], e, iv)
		if cost != 3*tok {
			t.Errorf("%s: interval ending %s has tokens %v but cost %v: a round was split", name, e, tok, cost)
		}
	}
	if output == "sum_samples_total" {
		for i := 1; i < len(got["tokens"]); i++ {
			if got["tokens"][i].value < got["tokens"][i-1].value {
				t.Fatalf("%s: the counter goes back from %v to %v", name, got["tokens"][i-1], got["tokens"][i])
			}
		}
	}

	// No sample is written before its interval: by the end of any interval, no
	// more has been written than was scraped by then.
	var sortedEnds []time.Duration
	for e := range ends {
		sortedEnds = append(sortedEnds, e)
	}
	sort.Slice(sortedEnds, func(i, j int) bool { return sortedEnds[i] < sortedEnds[j] })
	for _, e := range sortedEnds {
		var scraped float64
		for oe, v := range own {
			if oe <= e {
				scraped += v
			}
		}
		var written float64
		if output == "sum_samples" {
			written = sumIn(got["tokens"], -time.Hour, e)
		} else {
			written = increaseIn(got["tokens"], -time.Hour, e)
		}
		if written > scraped {
			t.Errorf("%s: %v written by %s, but only %v scraped by then", name, written, e, scraped)
		}
	}
	if overrun {
		return
	}

	// Where each push goes, from when it was processed: its own interval if
	// that has not ended by the boundary before, else the one it is processed in.
	want := map[time.Duration]float64{}
	for _, p := range pushes {
		want[max(end(min(p.sampleAt, p.at)), floor(p.at)+iv)] += p.value
	}
	for e := range ends {
		if g := perInterval(output, got["tokens"], e, iv); g != want[e] {
			t.Errorf("%s: interval ending %s has %v, want %v (scraped in it: %v)", name, e, g, want[e], own[e])
		}
	}
}

// A round still being processed when a flush walks the outputs reaches some of
// them before the flush does and some after: the ones reached after write it
// with the interval, the others hold it as the tail of the interval written
// last and write that again at the next flush. Either way the round is in the
// same interval in every output -- for a round of the interval being written,
// and for one processed late, whose interval is decided at its first sample.
func TestScrapeTimeIntervalsRoundDuringTheFlushStaysTogether(t *testing.T) {
	for _, c := range []struct {
		name               string
		scrapedAt, firstAt time.Duration
		want               map[int]float64
	}{
		{"of the interval being written", 100 * time.Second, 110 * time.Second, map[int]float64{2: 3}},
		{"late, begun before the flush", 10 * time.Second, 170 * time.Second, map[int]float64{3: 3}},
	} {
		for _, output := range stOutputs {
			got := roundDuringTheFlush(t, output, c.scrapedAt, c.firstAt)
			t.Run(c.name, func(t *testing.T) { checkMinutes(t, output, got, c.want, 5) })
		}
	}
}

// roundDuringTheFlush pushes the first part of a round scraped at scrapedAt at
// firstAt, and the rest of it once the next flush has written one output.
func roundDuringTheFlush(t *testing.T, output string, scrapedAt, firstAt time.Duration) map[string][]outSample {
	t.Helper()
	got := map[string][]outSample{}
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var mu sync.Mutex
		// The hook is in place before the flusher starts and gone after it
		// stops; armed and pushed are shared with it.
		var a *Aggregators
		var armed, pushed atomic.Bool
		var calls atomic.Int64
		var scraped int64
		flushEntryHook = func(*aggregator) {
			// the second entry of the flush: one output is written, the other not
			if armed.Load() && calls.Add(1) == 2 && !pushed.Swap(true) {
				a.Push(prometheus.MustParsePromMetrics(tokensCost(2), scraped), nil)
			}
		}
		defer func() { flushEntryHook = nil }()
		var err error
		a, err = LoadFromData([]byte(stConfig(output, "")), func(tss []prompb.TimeSeries) {
			mu.Lock()
			defer mu.Unlock()
			for _, ts := range tss {
				for _, l := range ts.Labels {
					if l.Name == "__name__" {
						for _, s := range ts.Samples {
							got[l.Value] = append(got[l.Value], outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
						}
					}
				}
			}
		}, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		scraped = start.Add(scrapedAt).UnixMilli()
		time.Sleep(firstAt)
		a.Push(prometheus.MustParsePromMetrics(tokensCost(1), scraped), nil)
		armed.Store(true)
		time.Sleep(5*time.Minute - time.Since(start))
		a.MustStop()
		if !pushed.Load() {
			t.Fatalf("no flush ran")
		}
	})
	for k := range got {
		sort.SliceStable(got[k], func(i, j int) bool { return got[k][i].at < got[k][j].at })
	}
	return got
}

// After sum_samples_total starts its total again, as it does before float64
// loses precision, a tail would be smaller than what was written at its
// interval and lost to deduplication; it goes to the next interval instead.
func TestScrapeTimeIntervalsTotalTailAfterRestartOfTheTotal(t *testing.T) {
	big := float64(1 << 53)
	got := runLateScenario(t, stConfig("sum_samples_total", ""), []latePush{
		{at: 90 * time.Second, sampleAt: 80 * time.Second, metrics: fmt.Sprintf("tokens %v", big)},
		{at: 2*time.Minute + 10*time.Second, sampleAt: 80 * time.Second, metrics: `tokens 5`},
	}, 4*time.Minute)["tokens"]
	var at2 []float64
	for _, s := range got {
		if s.at == 2*time.Minute {
			at2 = append(at2, s.value)
		}
	}
	if len(at2) != 1 || at2[0] != big {
		t.Fatalf("want only %v at 2m; got %v", big, got)
	}
	if v := got[len(got)-1].value; v != 5 {
		t.Fatalf("want the total started again at 5 after 2m; got %v", got)
	}
}

// Tokens and cost in rules of their own, as the _1m_by_* rules have them: each
// rule is an aggregator flushing on its own, and one of them can be well into
// the interval before its flush gets going. A round processed late in that
// time goes to the same interval in both: where a late round goes follows from
// when it is processed, decided once for every rule of the interval, not from
// how far each rule's flush has got.
func TestScrapeTimeIntervalsLateRoundSameIntervalAcrossRules(t *testing.T) {
	for _, output := range stOutputs {
		cfg := ""
		for _, name := range []string{"tokens", "cost"} {
			cfg += "- match: " + name + "\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
				"  staleness_interval: 40m\n  scrape_time_intervals: true\n"
		}
		got := map[string][]outSample{}
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			var mu sync.Mutex
			var a *Aggregators
			var held atomic.Bool
			// The tokens rule's flush at 3m is held for ten seconds before it
			// writes anything; the cost rule's goes ahead.
			flushEntryHook = func(ag *aggregator) {
				if ag == a.as[0] && time.Since(start) >= 3*time.Minute && !held.Swap(true) {
					time.Sleep(10 * time.Second)
				}
			}
			defer func() { flushEntryHook = nil }()
			var err error
			a, err = LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
				mu.Lock()
				defer mu.Unlock()
				for _, ts := range tss {
					for _, l := range ts.Labels {
						if l.Name == "__name__" {
							for _, s := range ts.Samples {
								got[l.Value] = append(got[l.Value], outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
							}
						}
					}
				}
			}, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			// an ordinary round first, so that both rules have an output to flush
			time.Sleep(10 * time.Second)
			a.Push(prometheus.MustParsePromMetrics(tokensCost(1), start.Add(5*time.Second).UnixMilli()), nil)
			time.Sleep(3*time.Minute + 5*time.Second - time.Since(start))
			if !held.Load() {
				t.Fatalf("the tokens rule's flush was not held")
			}
			// scraped in the minute ending 2m, processed in the one ending 4m
			a.Push(prometheus.MustParsePromMetrics(tokensCost(5), start.Add(70*time.Second).UnixMilli()), nil)
			time.Sleep(6 * time.Minute)
			a.MustStop()
		})
		for k := range got {
			sort.SliceStable(got[k], func(i, j int) bool { return got[k][i].at < got[k][j].at })
		}
		checkMinutes(t, output, got, map[int]float64{1: 1, 4: 5}, 8)
	}
}

// The tokens and the cost of every round in rules of their own, each flushing
// at its own pace -- a flush held up for a while before some of its outputs, in
// one rule and not the other -- and rounds processed up to two and a half
// minutes late, some while a flush walks the outputs: in every key and interval
// the cost is three times the tokens, across the two rules, and everything is
// counted once.
func TestScrapeTimeIntervalsRulesAcrossRules(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		for _, output := range stOutputs {
			for _, iv := range []time.Duration{time.Minute, 7 * time.Minute} {
				checkAcrossRules(t, seed, output, iv)
			}
		}
	}
}

func checkAcrossRules(t *testing.T, seed int64, output string, iv time.Duration) {
	t.Helper()
	name := fmt.Sprintf("seed=%d output=%s interval=%s", seed, output, iv)
	cfg := ""
	for _, m := range []string{"tokens", "cost"} {
		cfg += fmt.Sprintf("- match: %s\n  interval: %s\n  by: [key]\n  outputs: [%s]\n  staleness_interval: 2h\n  flush_on_shutdown: true\n  scrape_time_intervals: true\n", m, iv, output)
	}
	rnd := rand.New(rand.NewSource(seed))
	type push struct {
		at, sampleAt time.Duration
		key          int
		value        float64
		split        time.Duration // the cost pushed this much after the tokens, 0 for one block
	}
	const minutes = 40
	var pushes []push
	offsets := []time.Duration{0, time.Millisecond, 13 * time.Second, 37 * time.Second, 59*time.Second + 999*time.Millisecond}
	for m := 1; m < minutes; m++ {
		for _, off := range offsets {
			sampleAt := time.Duration(m)*time.Minute + off
			delay := time.Duration(1+rnd.Intn(2000)) * time.Millisecond
			if rnd.Intn(4) == 0 {
				delay = time.Duration(rnd.Intn(150))*time.Second + time.Duration(1+rnd.Intn(999))*time.Millisecond
			}
			// A round is pushed in blocks: often its tokens in one and its cost in
			// another, seconds later, which a boundary can fall between.
			split := time.Duration(0)
			if rnd.Intn(2) == 0 {
				split = time.Duration(1+rnd.Intn(8000)) * time.Millisecond
			}
			pushes = append(pushes, push{at: sampleAt + delay, sampleAt: sampleAt, key: rnd.Intn(3), value: float64(1 + rnd.Intn(9)), split: split})
		}
	}
	sort.SliceStable(pushes, func(i, j int) bool { return pushes[i].at < pushes[j].at })
	holds := make([]time.Duration, 512) // what each entry flush in turn is held for, by rule
	for i := range holds {
		if rnd.Intn(5) == 0 {
			holds[i] = time.Duration(1+rnd.Intn(30)) * time.Second
		}
	}

	got := map[string][]outSample{} // by metric and key
	var start time.Time
	synctest.Test(t, func(t *testing.T) {
		start = time.Now()
		var mu sync.Mutex
		var a *Aggregators
		var calls [2]atomic.Int64
		flushEntryHook = func(ag *aggregator) {
			for i := range calls {
				if ag == a.as[i] {
					n := calls[i].Add(1)
					time.Sleep(holds[(int(n)*2+i)%len(holds)])
				}
			}
		}
		defer func() { flushEntryHook = nil }()
		var err error
		a, err = LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
			mu.Lock()
			defer mu.Unlock()
			for _, ts := range tss {
				var metric, key string
				for _, l := range ts.Labels {
					switch l.Name {
					case "__name__":
						metric = l.Value
					case "key":
						key = l.Value
					}
				}
				if i := strings.IndexByte(metric, ':'); i > 0 {
					metric = metric[:i]
				}
				for _, s := range ts.Samples {
					got[metric+"/"+key] = append(got[metric+"/"+key], outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
				}
			}
		}, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		// the pushes in the order they are made, a split round's two blocks apart
		type block struct {
			at, sampleAt time.Duration
			text         string
		}
		var blocks []block
		for _, p := range pushes {
			tok := fmt.Sprintf("tokens{key=\"k%d\"} %v", p.key, p.value)
			cost := fmt.Sprintf("cost{key=\"k%d\"} %v", p.key, 3*p.value)
			if p.split == 0 {
				blocks = append(blocks, block{p.at, p.sampleAt, tok + "\n" + cost})
			} else {
				blocks = append(blocks, block{p.at, p.sampleAt, tok}, block{p.at + p.split, p.sampleAt, cost})
			}
		}
		sort.SliceStable(blocks, func(i, j int) bool { return blocks[i].at < blocks[j].at })
		for _, b := range blocks {
			time.Sleep(start.Add(b.at).Sub(time.Now()))
			a.Push(prometheus.MustParsePromMetrics(b.text, start.Add(b.sampleAt).UnixMilli()), nil)
		}
		time.Sleep(start.Add(minutes*time.Minute + 5*time.Minute).Sub(time.Now()))
		a.MustStop()
	})
	for k := range got {
		sort.SliceStable(got[k], func(i, j int) bool { return got[k][i].at < got[k][j].at })
	}
	all := map[int]float64{}
	for _, p := range pushes {
		all[p.key] += p.value
	}
	ends := map[time.Duration]bool{}
	for k := range got {
		for _, s := range got[k] {
			ends[s.at.Truncate(time.Second)] = true
		}
	}
	total := func(samples []outSample) float64 {
		if output == "sum_samples" {
			return sumIn(samples, -time.Hour, 2*time.Hour)
		}
		return increaseIn(samples, -time.Hour, 2*time.Hour)
	}
	for key := 0; key < 3; key++ {
		tok, cost := got[fmt.Sprintf("tokens/k%d", key)], got[fmt.Sprintf("cost/k%d", key)]
		if tt, cc := total(tok), total(cost); tt != all[key] || cc != 3*all[key] {
			t.Errorf("%s: key k%d: tokens %v, cost %v; want %v and %v", name, key, tt, cc, all[key], 3*all[key])
		}
		for e := range ends {
			a, b := perInterval(output, tok, e, iv), perInterval(output, cost, e, iv)
			if b != 3*a {
				t.Errorf("%s: key k%d: interval ending %s has tokens %v but cost %v: a round was split between the rules", name, key, e, a, b)
			}
		}
		if output == "sum_samples_total" {
			for _, s := range [][]outSample{tok, cost} {
				for i := 1; i < len(s); i++ {
					if s[i].value < s[i-1].value {
						t.Fatalf("%s: key k%d: the counter goes back from %v to %v", name, key, s[i-1], s[i])
					}
				}
			}
		}
	}
}

// A flush that runs over its interval leaves the next tick waiting, and the
// flusher goes on with the next boundary right away -- before it is reached. A
// rule must not take an interval for written before it has ended: a round
// processed then would be the tail of the interval written last in the other
// rules and late in this one, two intervals apart.
func TestScrapeTimeIntervalsEarlyFlushStaysBehindTheClock(t *testing.T) {
	// Aligned to the interval, the flusher drops the tick that waited; not
	// aligned, it takes it and flushes at once, for a boundary not yet reached.
	for _, align := range []string{"", "  no_align_flush_to_interval: true\n"} {
		for _, output := range stOutputs {
			earlyFlush(t, output, align)
		}
	}
}

func earlyFlush(t *testing.T, output, align string) {
	t.Helper()
	{
		cfg := ""
		for _, name := range []string{"tokens", "cost"} {
			cfg += "- match: " + name + "\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
				"  staleness_interval: 40m\n  scrape_time_intervals: true\n" + align
		}
		got := map[string][]outSample{}
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			var mu sync.Mutex
			var a *Aggregators
			var held atomic.Bool
			// The tokens rule's flush at 1m takes 90 seconds; the one at 2m then
			// runs as soon as it is done, at 2m30s, for the boundary at 3m.
			flushEntryHook = func(ag *aggregator) {
				if ag == a.as[0] && !held.Swap(true) {
					time.Sleep(90 * time.Second)
				}
			}
			defer func() { flushEntryHook = nil }()
			var err error
			a, err = LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
				mu.Lock()
				defer mu.Unlock()
				for _, ts := range tss {
					for _, l := range ts.Labels {
						if l.Name == "__name__" {
							for _, s := range ts.Samples {
								got[l.Value] = append(got[l.Value], outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
							}
						}
					}
				}
			}, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			time.Sleep(10 * time.Second)
			a.Push(prometheus.MustParsePromMetrics(tokensCost(1), start.Add(5*time.Second).UnixMilli()), nil)
			// a round of the minute ending 3m, before the early flush
			time.Sleep(140*time.Second - time.Since(start))
			a.Push(prometheus.MustParsePromMetrics(tokensCost(2), start.Add(135*time.Second).UnixMilli()), nil)
			// a round scraped in the minute ending 2m, processed at 2m40s, after its
			// minute ended: the minute it is processed in, in every rule
			time.Sleep(160*time.Second - time.Since(start))
			a.Push(prometheus.MustParsePromMetrics(tokensCost(5), start.Add(110*time.Second).UnixMilli()), nil)
			time.Sleep(7*time.Minute - time.Since(start))
			a.MustStop()
		})
		for k := range got {
			sort.SliceStable(got[k], func(i, j int) bool { return got[k][i].at < got[k][j].at })
		}
		checkMinutes(t, output, got, map[int]float64{1: 1, 3: 2 + 5}, 7)
		for _, s := range got["tokens"] {
			if s.at%time.Minute != 0 {
				t.Errorf("%s %q: %v written inside an interval, before it ended", output, align, s)
			}
		}
	}
}

// A round whose interval has ended by the time it is processed, its tokens in
// one block before a boundary and its cost in another after it, in rules of
// their own: the round goes whole to one interval. Its interval is decided at
// its first block and kept for the rest, rather than the first block being the
// tail of its own interval and the second, a boundary later, late.
func TestScrapeTimeIntervalsRoundInBlocksAcrossABoundary(t *testing.T) {
	for _, output := range stOutputs {
		cfg := ""
		for _, name := range []string{"tokens", "cost"} {
			cfg += "- match: " + name + "\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
				"  staleness_interval: 40m\n  scrape_time_intervals: true\n"
		}
		scraped := 70 * time.Second // in the minute ending 2m
		got := runLateScenario(t, cfg, []latePush{
			{at: 10 * time.Second, sampleAt: 5 * time.Second, metrics: tokensCost(1)},
			{at: 150 * time.Second, sampleAt: scraped, metrics: `tokens 5`},
			{at: 185 * time.Second, sampleAt: scraped, metrics: `cost 15`},
		}, 6*time.Minute)
		checkMinutes(t, output, got, map[int]float64{1: 1, 3: 5}, 6)
	}
}

// A push decided before a flush and adding to an output after the flush has
// written it -- a boundary between the two -- is taken for one decided after:
// the flush waits for the pushes in progress. And every rule places a push by
// the same reading of the clock, however long the push takes to go from one
// rule to the next.
func TestScrapeTimeIntervalsPushAcrossTheFlush(t *testing.T) {
	for _, output := range stOutputs {
		cfg := ""
		for _, name := range []string{"tokens", "cost"} {
			cfg += "- match: " + name + "\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
				"  staleness_interval: 40m\n  scrape_time_intervals: true\n"
		}
		got := map[string][]outSample{}
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			var mu sync.Mutex
			var a *Aggregators
			var holdPush, holdFlush atomic.Bool
			// The push is held for five seconds in the tokens rule, after it has
			// been placed, from just before the boundary at 3m to after it; the
			// cost rule's flush at 3m is held for ten.
			pushHook = func(ag *aggregator) {
				if ag == a.as[0] && holdPush.Load() && holdPush.CompareAndSwap(true, false) {
					time.Sleep(5 * time.Second)
				}
			}
			flushEntryHook = func(ag *aggregator) {
				if ag == a.as[1] && time.Since(start) >= 3*time.Minute && !holdFlush.Swap(true) {
					time.Sleep(10 * time.Second)
				}
			}
			defer func() { pushHook, flushEntryHook = nil, nil }()
			var err error
			a, err = LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
				mu.Lock()
				defer mu.Unlock()
				for _, ts := range tss {
					for _, l := range ts.Labels {
						if l.Name == "__name__" {
							for _, s := range ts.Samples {
								got[l.Value] = append(got[l.Value], outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
							}
						}
					}
				}
			}, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			scraped := start.Add(100 * time.Second).UnixMilli() // in the minute ending 2m
			time.Sleep(110 * time.Second)
			a.Push(prometheus.MustParsePromMetrics(tokensCost(1), scraped), nil)
			// the rest of the round, a millisecond before the boundary at 3m
			time.Sleep(3*time.Minute - time.Millisecond - time.Since(start))
			holdPush.Store(true)
			a.Push(prometheus.MustParsePromMetrics(tokensCost(2), scraped), nil)
			time.Sleep(6*time.Minute - time.Since(start))
			a.MustStop()
		})
		for k := range got {
			sort.SliceStable(got[k], func(i, j int) bool { return got[k][i].at < got[k][j].at })
		}
		checkMinutes(t, output, got, map[int]float64{2: 3}, 6)
	}
}

// On shutdown every rule writes what it holds, capped at the time of the stop:
// one time for all of them, or a round whose tokens and cost are in rules of
// their own is written at two timestamps a few milliseconds apart, and a query
// at either one sees half of it. The tokens rule's last flush is held up for a
// few milliseconds, as one rule's flush is behind another's on a real stop.
func TestScrapeTimeIntervalsShutdownWritesEveryRuleAtOneTime(t *testing.T) {
	for _, output := range stOutputs {
		cfg := ""
		for _, name := range []string{"tokens", "cost"} {
			cfg += "- match: " + name + "\n  interval: 1m\n  outputs: [" + output + "]\n  keep_metric_names: true\n" +
				"  staleness_interval: 40m\n  flush_on_shutdown: true\n  scrape_time_intervals: true\n"
		}
		got := map[string][]outSample{}
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			var mu sync.Mutex
			var a *Aggregators
			var held atomic.Bool
			flushEntryHook = func(ag *aggregator) {
				if ag == a.as[0] && time.Since(start) >= 80*time.Second && !held.Swap(true) {
					time.Sleep(3 * time.Millisecond)
				}
			}
			defer func() { flushEntryHook = nil }()
			var err error
			a, err = LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
				mu.Lock()
				defer mu.Unlock()
				for _, ts := range tss {
					for _, l := range ts.Labels {
						if l.Name == "__name__" {
							for _, s := range ts.Samples {
								got[l.Value] = append(got[l.Value], outSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
							}
						}
					}
				}
			}, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			time.Sleep(10 * time.Second)
			a.Push(prometheus.MustParsePromMetrics(tokensCost(1), start.Add(5*time.Second).UnixMilli()), nil)
			// in the interval ending 2m, which has not ended when vmagent stops
			time.Sleep(70*time.Second - time.Since(start))
			a.Push(prometheus.MustParsePromMetrics(tokensCost(5), start.Add(65*time.Second).UnixMilli()), nil)
			time.Sleep(80*time.Second - time.Since(start))
			a.MustStop()
			if !held.Load() {
				t.Fatalf("the tokens rule's last flush was not held")
			}
		})
		for k := range got {
			sort.SliceStable(got[k], func(i, j int) bool { return got[k][i].at < got[k][j].at })
		}
		if len(got["tokens"]) == 0 || len(got["tokens"]) != len(got["cost"]) {
			t.Fatalf("%s: tokens %v, cost %v", output, got["tokens"], got["cost"])
		}
		for i, tok := range got["tokens"] {
			if c := got["cost"][i]; c.at != tok.at || c.value != 3*tok.value {
				t.Errorf("%s: tokens %v at %v, cost %v at %v: the round was written at two times; tokens %v, cost %v",
					output, tok.value, tok.at, c.value, c.at, got["tokens"], got["cost"])
			}
		}
		if last := got["tokens"][len(got["tokens"])-1]; last.at > 80*time.Second+3*time.Millisecond {
			t.Errorf("%s: the last round was written at %v, after the stop", output, last.at)
		}
	}
}

// MustStop waits for a push already let through to the aggregators and not at
// them yet, and its samples are in the last flush; a push after that is told
// the aggregators stopped, and takes nothing.
func TestMustStopWaitsForThePushesInProgress(t *testing.T) {
	cfg := "- match: tokens\n  interval: 1m\n  outputs: [sum_samples]\n  keep_metric_names: true\n  flush_on_shutdown: true\n"
	var mu sync.Mutex
	var got []float64
	a, err := LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
		mu.Lock()
		defer mu.Unlock()
		for _, ts := range tss {
			for _, s := range ts.Samples {
				got = append(got, s.Value)
			}
		}
	}, nil, "test")
	if err != nil {
		t.Fatalf("cannot load config: %s", err)
	}
	reached, release := make(chan struct{}), make(chan struct{})
	tryPushHook = func() { close(reached); <-release }
	defer func() { tryPushHook = nil }()
	pushed := make(chan bool)
	go func() {
		_, _, ok := a.TryPush(prometheus.MustParsePromMetrics("tokens 5", 1000), nil)
		pushed <- ok
	}()
	<-reached
	tryPushHook = nil
	stopped := make(chan struct{})
	go func() { a.MustStop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatalf("MustStop returned with a push in progress")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if !<-pushed {
		t.Fatalf("the push in progress was refused")
	}
	<-stopped
	if _, _, ok := a.TryPush(prometheus.MustParsePromMetrics("tokens 7", 1000), nil); ok {
		t.Fatalf("a push after MustStop was taken")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != 5 {
		t.Fatalf("written %v; want the push in progress, 5, in the last flush", got)
	}
}

// Under scrape_time_intervals a rule flushes on the grid of its own interval,
// whenever the process started and flush_on_shutdown or not: an interval is
// written as soon as it ends, not up to an interval later at the phase of the
// start, and a process killed outright takes the interval in progress with it,
// not two. A rule without scrape_time_intervals keeps the phase of the start.
func TestScrapeTimeIntervalsFlushOnTheGridOfTheInterval(t *testing.T) {
	cfg := "- match: tokens\n  interval: 1m\n  outputs: [sum_samples]\n  flush_on_shutdown: true\n  scrape_time_intervals: true\n" +
		"- match: cost\n  interval: 5m\n  outputs: [sum_samples]\n  flush_on_shutdown: true\n  scrape_time_intervals: true\n" +
		"- match: reqs\n  interval: 1m\n  outputs: [sum_samples]\n  flush_on_shutdown: true\n  scrape_time_intervals: false\n"
	flushes := map[string][]time.Duration{} // per input metric, when its outputs were written
	synctest.Test(t, func(t *testing.T) {
		epoch := time.Now() // synctest starts on a whole hour
		time.Sleep(28 * time.Second)
		var mu sync.Mutex
		a, err := LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
			mu.Lock()
			defer mu.Unlock()
			at := time.Since(epoch)
			for _, ts := range tss {
				for _, l := range ts.Labels {
					if l.Name == "__name__" {
						name, _, _ := strings.Cut(l.Value, ":")
						if s := flushes[name]; len(s) == 0 || s[len(s)-1] != at {
							flushes[name] = append(s, at)
						}
					}
				}
			}
		}, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		for i := 0; i < 70; i++ {
			now := time.Now()
			a.Push(prometheus.MustParsePromMetrics("tokens 1\ncost 1\nreqs 1", now.Truncate(10*time.Second).UnixMilli()), nil)
			time.Sleep(10 * time.Second)
		}
		a.MustStop()
	})
	check := func(name string, every, phase time.Duration) {
		t.Helper()
		got := flushes[name]
		if len(got) < 2 {
			t.Fatalf("%s: written at %v; want flushes", name, got)
		}
		for _, at := range got[:len(got)-1] { // the last is the flush on shutdown
			if at%every != phase {
				t.Errorf("%s: written at %v, at %v into its %v interval; want %v; all %v", name, at, at%every, every, phase, got)
			}
		}
	}
	check("tokens", time.Minute, 0)
	check("cost", 5*time.Minute, 0)
	check("reqs", time.Minute, 28*time.Second)
}

// sample_timestamps, the former name of scrape_time_intervals, still works the
// same, and the two set to different values are refused.
func TestScrapeTimeIntervalsFormerName(t *testing.T) {
	rule := func(extra string) string {
		return "- match: tokens\n  interval: 1m\n  outputs: [sum_samples]\n" + extra
	}
	for _, c := range []struct {
		extra  string
		on, ok bool
	}{
		{"  scrape_time_intervals: true\n", true, true},
		{"  sample_timestamps: true\n", true, true},
		{"  sample_timestamps: false\n", false, true},
		{"  scrape_time_intervals: true\n  sample_timestamps: true\n", true, true},
		{"  scrape_time_intervals: true\n  sample_timestamps: false\n", false, false},
	} {
		a, err := LoadFromData([]byte(rule(c.extra)), func([]prompb.TimeSeries) {}, nil, "test")
		if (err == nil) != c.ok {
			t.Fatalf("%q: error %v; want ok %v", c.extra, err, c.ok)
		}
		if err != nil {
			continue
		}
		if got := a.as[0].scrapeTimeIntervals; got != c.on {
			t.Errorf("%q: scrape_time_intervals %v; want %v", c.extra, got, c.on)
		}
		a.MustStop()
	}
}

// The flush ticker keeps the monotonic clock, and the wall clock drifts from it
// and is corrected against it: here the wall clock is 50 ms behind, so every
// tick comes before the boundary by the wall clock. An interval is still written
// at the flush right after it ends, not left to the next one an interval later.
func TestScrapeTimeIntervalsFlushWaitsForTheWallClock(t *testing.T) {
	cfg := "- match: tokens\n  interval: 1m\n  outputs: [sum_samples]\n  flush_on_shutdown: true\n  scrape_time_intervals: true\n"
	type write struct{ at, ts time.Duration }
	var got []write
	synctest.Test(t, func(t *testing.T) {
		epoch := time.Now()
		stClock = func() time.Time { return time.Now().Add(-50 * time.Millisecond) }
		defer func() { stClock = time.Now }()
		time.Sleep(28 * time.Second)
		var mu sync.Mutex
		a, err := LoadFromData([]byte(cfg), func(tss []prompb.TimeSeries) {
			mu.Lock()
			defer mu.Unlock()
			for _, ts := range tss {
				for _, s := range ts.Samples {
					got = append(got, write{time.Since(epoch), time.UnixMilli(s.Timestamp).Sub(epoch)})
				}
			}
		}, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		for m := 1; m <= 6; m++ {
			time.Sleep(time.Duration(m)*time.Minute + 3*time.Second - time.Since(epoch))
			a.Push(prometheus.MustParsePromMetrics("tokens 1", epoch.Add(time.Duration(m)*time.Minute+time.Second).UnixMilli()), nil)
		}
		time.Sleep(7*time.Minute + 10*time.Second - time.Since(epoch))
		a.MustStop()
	})
	for _, w := range got {
		if w.ts == 7*time.Minute+10*time.Second-50*time.Millisecond { // the flush on shutdown
			continue
		}
		if late := w.at - w.ts; late < 0 || late >= time.Second {
			t.Errorf("the interval ending %v was written at %v, %v after its end; want at the flush right after it; all %v", w.ts, w.at, late, got)
		}
	}
	if len(got) < 5 {
		t.Fatalf("written %v; want the six intervals", got)
	}
}
