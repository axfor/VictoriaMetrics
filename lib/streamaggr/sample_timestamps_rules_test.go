package streamaggr

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
)

// The rules of sample_timestamps, as these tests check them. A sample belongs
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
		"  staleness_interval: 40m\n  sample_timestamps: true\n" + extra
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
func TestSampleTimestampsOnTimeRoundsGoToTheirInterval(t *testing.T) {
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
func TestSampleTimestampsBoundaryBelongsToTheIntervalItStarts(t *testing.T) {
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
func TestSampleTimestampsTailCompletesTheIntervalWrittenLast(t *testing.T) {
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
func TestSampleTimestampsNegativeTailGoesToTheNextInterval(t *testing.T) {
	got := runLateScenario(t, stConfig("sum_samples", ""), []latePush{
		{at: 90 * time.Second, sampleAt: 80 * time.Second, metrics: `tokens 4`},
		{at: 2*time.Minute + 10*time.Second, sampleAt: 80 * time.Second, metrics: `tokens -1`},
	}, 4*time.Minute)["tokens"]
	if a, b := sumIn(got, time.Minute, 2*time.Minute), sumIn(got, 2*time.Minute, 3*time.Minute); a != 4 || b != -1 {
		t.Fatalf("want 4 in the minute ending 2m and -1 in the next; got %v and %v: %v", a, b, got)
	}
}

// Rounds processed after a later interval was written go to the first interval
// not written, all of them together; the round of the interval written last is
// its tail. Nothing is lost, nothing counted twice.
func TestSampleTimestampsLateRoundsGoToTheFirstIntervalNotWritten(t *testing.T) {
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
		checkMinutes(t, output, got, map[int]float64{5: 4, 6: 1 + 2 + 3, 7: 20}, 8)
	}
}

// The rest of a late round processed after the flush of the interval it was
// put in goes there too, as that interval's tail: samples sharing a scrape
// timestamp are taken as one round, whose interval is decided at the first.
func TestSampleTimestampsLateRoundKeepsItsInterval(t *testing.T) {
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
func TestSampleTimestampsRoundsBeforeTheStartGoToTheFirstInterval(t *testing.T) {
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
func TestSampleTimestampsFutureTimestampCountsNow(t *testing.T) {
	got := runLateScenario(t, stConfig("sum_samples", "  flush_on_shutdown: true\n"), []latePush{
		{at: 10 * time.Second, sampleAt: 2*time.Minute + 30*time.Second, metrics: `tokens 5`},
	}, 4*time.Minute)["tokens"]
	if sumIn(got, 0, time.Minute) != 5 || len(got) != 1 {
		t.Fatalf("want 5 written once in the minute ending 1m; got %v", got)
	}
}

// sum_samples_total never goes back, puts the reset marker before its first
// sample, and its heartbeat does not get ahead of the intervals written.
func TestSampleTimestampsTotalStaysACounter(t *testing.T) {
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
	// the round of the minute ending 12m is its tail; the ten before it are late
	checkMinutes(t, "sum_samples_total", got, map[int]float64{1: 1, 12: 1, 13: 10}, 14)
}

// However far behind the aggregator is, an output holds only a few sums: the
// samples of intervals before the one written last go to the first one not
// written. What is left is that one, the tail of the one written last and, for
// sum_samples, what was written for it, and the next one.
func TestSampleTimestampsPendingStaysBounded(t *testing.T) {
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
func TestSampleTimestampsRules(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		for _, output := range stOutputs {
			for _, iv := range []time.Duration{time.Minute, 7 * time.Minute} {
				for _, overrun := range []bool{false, true} {
					checkSampleTimestampsRules(t, seed, output, iv, overrun)
				}
			}
		}
	}
}

func checkSampleTimestampsRules(t *testing.T, seed int64, output string, iv time.Duration, overrun bool) {
	t.Helper()
	name := fmt.Sprintf("seed=%d output=%s interval=%s overrun=%v", seed, output, iv, overrun)
	cfg := fmt.Sprintf("- match: '{__name__=~\"tokens|cost\"}'\n  interval: %s\n  outputs: [%s]\n  keep_metric_names: true\n"+
		"  staleness_interval: 2h\n  flush_on_shutdown: true\n  sample_timestamps: true\n", iv, output)
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

	// Where each push goes, from when it was processed: the flushes run at the
	// boundaries, and at once.
	first := floor(0) // the interval before the start counts as written, by another process
	want := map[time.Duration]float64{}
	for _, p := range pushes {
		e := end(min(p.sampleAt, p.at))
		written := floor(p.at)
		switch {
		case e > written:
		case e == written && written > first:
		default:
			e = written + iv
		}
		want[e] += p.value
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
func TestSampleTimestampsRoundDuringTheFlushStaysTogether(t *testing.T) {
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
		var scraped int64
		flushEntryHook = func() {
			if armed.Load() && !pushed.Swap(true) {
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
func TestSampleTimestampsTotalTailAfterRestartOfTheTotal(t *testing.T) {
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
