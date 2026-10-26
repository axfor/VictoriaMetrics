package streamaggr

import (
	"cmp"
	"math"
	"slices"
	"sync"
)

type sumSamplesAggrValue struct {
	sum float64
}

func (av *sumSamplesAggrValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	av.sum += sample.value
}

func (av *sumSamplesAggrValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	ctx.appendSeries(key, "sum_samples", av.sum)
	av.sum = 0
}

func (*sumSamplesAggrValue) state() any {
	return nil
}

func newSumSamplesAggrConfig(scrapeTimeIntervals bool) aggrConfig {
	return &sumSamplesAggrConfig{scrapeTimeIntervals: scrapeTimeIntervals}
}

type sumSamplesAggrConfig struct {
	scrapeTimeIntervals bool
}

func (c *sumSamplesAggrConfig) getValue(_ any) aggrValue {
	if c.scrapeTimeIntervals {
		return &sumSamplesAtValue{}
	}
	return &sumSamplesAggrValue{}
}

// sumSamplesAtValue is sum_samples under scrape_time_intervals: every sum goes to
// the interval its samples were scraped in, written at the end of it, rather
// than to the one the aggregator happened to process them in. See stPlace for
// where a sample goes and stFlush for when it is written.
type sumSamplesAtValue struct {
	pending bySampleTime
}

func (av *sumSamplesAtValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	av.pending.add(sample.timestamp, sample.value)
}

// flush writes every interval closed by this flush at its end, once, with
// everything the aggregator has for it. The interval written last is kept, so
// that a sample of it processed after the flush -- the tail of a round crossing
// the boundary -- makes the next flush write it again, complete, at the same
// timestamp; VM's deduplication keeps the bigger of the two, which is the
// complete one, as long as what is added is not negative. Anything else
// arriving for an interval already written goes to the first one not written.
func (av *sumSamplesAtValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	if av.pending.empty() {
		return
	}
	st := &ctx.st
	slots := av.pending.take(ctx.slots[:0])
	defer func() { ctx.slots = slots[:0] }()
	labelsLen, samplesLen, started := 0, 0, false
	write := func(at int64, v float64) {
		if !started {
			labelsLen, samplesLen = ctx.beginSeries(key, "sum_samples")
			started = true
		}
		ctx.appendSample(min(at, ctx.now), v)
	}

	// The interval written last, if this output wrote it: its sum, kept for its tail.
	var last slot
	for i := range slots {
		if slots[i].ts < 0 {
			last = slot{-slots[i].ts, slots[i].sum}
			slots[i].ts = 0
		}
	}
	if last.ts != st.written {
		last = slot{}
	}
	var late float64
	lateAny := false
	for i := range slots {
		x := &slots[i]
		switch {
		case x.ts == 0 || x.ts > st.written:
		case x.ts == st.written && st.tailOK && x.sum >= 0:
			// The tail of the interval written last: written again, complete.
			last = slot{x.ts, last.sum + x.sum}
			write(last.ts, last.sum)
			x.ts = 0
		default:
			// Too late for its own interval: the first one not written takes it.
			late, lateAny = late+x.sum, true
			x.ts = 0
		}
	}
	if lateAny {
		slots = append(slots, slot{st.written + ctx.a.interval.Milliseconds(), late})
	}
	slices.SortFunc(slots, func(a, b slot) int { return cmp.Compare(a.ts, b.ts) })
	for _, x := range mergeSlots(slots) {
		if x.ts > st.closing {
			av.pending.add(x.ts, x.sum)
			continue
		}
		write(x.ts, x.sum)
		last = x
	}
	if !ctx.isLast && last.ts != 0 && last.ts == max(st.closing, st.written) {
		av.pending.setWritten(last.ts, last.sum)
	}
	if started {
		ctx.endSeries(labelsLen, samplesLen)
	}
}

func (*sumSamplesAtValue) state() any { return nil }

func (av *sumSamplesAtValue) firstPending() (int64, bool) { return av.pending.first() }

// slot is the sum of one interval, under its end. A negative end marks the
// interval written last, kept for its tail, see sumSamplesAtValue.flush.
type slot struct {
	ts  int64
	sum float64
}

// bySampleTime holds the input sums of one output, one per interval, for
// scrape_time_intervals. Two are inline, which covers what an output has at any
// time but while the aggregator is far behind on flushing: the interval being
// filled, and the one written last -- or the next one, which the rounds right
// after a boundary start before the flush gets to the output.
type bySampleTime struct {
	a, b slot
	more *[]slot
}

// tsSumsPool keeps the slices of the outputs that held more than two intervals.
var tsSumsPool = sync.Pool{New: func() any { return &[]slot{} }}

// empty reports whether nothing is held, written or not.
func (p *bySampleTime) empty() bool { return p.a.ts == 0 && p.b.ts == 0 && p.more == nil }

func (p *bySampleTime) add(ts int64, v float64) {
	switch {
	case p.a.ts == ts:
		p.a.sum += v
		return
	case p.b.ts == ts:
		p.b.sum += v
		return
	}
	if p.more != nil {
		for i := range *p.more {
			if (*p.more)[i].ts == ts {
				(*p.more)[i].sum += v
				return
			}
		}
	}
	switch {
	case p.a.ts == 0:
		p.a = slot{ts, v}
	case p.b.ts == 0:
		p.b = slot{ts, v}
	default:
		if p.more == nil {
			p.more = tsSumsPool.Get().(*[]slot)
		}
		*p.more = append(*p.more, slot{ts, v})
	}
}

// setWritten keeps the sum of the interval ending at ts as the one written last.
func (p *bySampleTime) setWritten(ts int64, sum float64) { p.add(-ts, sum) }

// take appends everything held to dst and empties p.
func (p *bySampleTime) take(dst []slot) []slot {
	for _, x := range [2]slot{p.a, p.b} {
		if x.ts != 0 {
			dst = append(dst, x)
		}
	}
	if p.more != nil {
		dst = append(dst, *p.more...)
		*p.more = (*p.more)[:0]
		tsSumsPool.Put(p.more)
	}
	*p = bySampleTime{}
	return dst
}

// first is the earliest interval with input not yet written.
func (p *bySampleTime) first() (int64, bool) {
	first, ok := int64(0), false
	visit := func(x slot) {
		if x.ts > 0 && (!ok || x.ts < first) {
			first, ok = x.ts, true
		}
	}
	visit(p.a)
	visit(p.b)
	if p.more != nil {
		for _, x := range *p.more {
			visit(x)
		}
	}
	return first, ok
}

// mergeSlots adds up the slots of one interval, in a slice sorted by end.
func mergeSlots(s []slot) []slot {
	n := 0
	for _, x := range s {
		if x.ts == 0 {
			continue
		}
		if n > 0 && s[n-1].ts == x.ts {
			s[n-1].sum += x.sum
			continue
		}
		s[n] = x
		n++
	}
	return s[:n]
}

// stState is the aggregator's position in the intervals under
// scrape_time_intervals, as of the start of a flush. Every output of the aggregator
// decides with the same one, so a round's samples go to the same interval in
// all of them.
type stState struct {
	// written is the end of the interval written last: every interval ending at
	// or before it has been written. tailOK is false until this process has
	// written one -- at start, the interval before is the previous process's,
	// which may have written it, and a value written again at its timestamp would
	// be deduplicated against that one instead of completing it.
	written int64
	tailOK  bool
	// closing is the end of the last interval this flush writes; the intervals
	// after it are kept for later flushes. On the flush at shutdown it is
	// math.MaxInt64: everything is written.
	closing int64
}

// sumSamplesTotalAggrValue sums input sample values like sum_samples, but doesn't reset the sum on flush,
// so the output is a cumulative counter.
//
// It is intended for inputs which already are increments, e.g. scrape targets exposing the increase since the last
// delivered scrape. Unlike total, it keeps no state per input series, so its memory usage doesn't depend on
// the number of inputs merged into every output, and a restart doesn't need a baseline sample per input.
type sumSamplesTotalAggrValue struct {
	sum    float64
	shared *sumSamplesTotalShared
}

type sumSamplesTotalShared struct {
	total float64

	// lastWritten is the value the last flush actually wrote, and lastWrittenAt
	// when. They are only consulted under output_heartbeat_interval: a flush
	// whose total equals lastWritten and that falls within the heartbeat is
	// skipped, because the storage already holds that value.
	//
	// Skipping is safe here and would not be for an output that resets every
	// interval: a change is always written, so a gap spans an interval over
	// which the value did not move, the samples on either side of it are equal,
	// and increase() over any window containing the gap is unaffected.
	lastWritten   float64
	lastWrittenAt int64
	written       bool
}

func (av *sumSamplesTotalAggrValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	av.sum += sample.value
}

func (av *sumSamplesTotalAggrValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	total := av.shared.total + av.sum
	av.sum = 0
	if math.Abs(total) >= (1 << 53) {
		// It is time to reset the entry, since it starts losing float64 precision
		av.shared.total = 0
	} else {
		av.shared.total = total
	}
	if av.skip(ctx, total) {
		ctx.a.skippedOutputs.Inc()
		return
	}
	av.shared.lastWritten = total
	av.shared.lastWrittenAt = ctx.flushTimestamp
	av.shared.written = true
	ctx.appendSeries(key, "sum_samples_total", total)
}

// skip reports whether this flush writes nothing because the value has not
// moved since the last one written and the heartbeat is not due.
//
// The flush at shutdown is not special-cased. Writing it again would change
// nothing observable: if the value moved it is written like any other change,
// and if it did not, the storage already holds it at an earlier timestamp,
// which is where it has been true since.
func (av *sumSamplesTotalAggrValue) skip(ctx *flushCtx, total float64) bool {
	hb := ctx.a.outputHeartbeatInterval
	if hb <= 0 || !av.shared.written {
		return false
	}
	if total != av.shared.lastWritten {
		return false
	}
	return ctx.flushTimestamp-av.shared.lastWrittenAt < hb.Milliseconds()
}

func (av *sumSamplesTotalAggrValue) state() any {
	return av.shared
}

func newSumSamplesTotalAggrConfig(scrapeTimeIntervals bool) aggrConfig {
	return &sumSamplesTotalAggrConfig{scrapeTimeIntervals: scrapeTimeIntervals}
}

type sumSamplesTotalAggrConfig struct {
	scrapeTimeIntervals bool
}

// sumSamplesTotalOwned is a value together with the shared state it owns when
// no other value shares it, so the pair costs one allocation instead of two.
type sumSamplesTotalOwned struct {
	v      sumSamplesTotalAggrValue
	shared sumSamplesTotalShared
}

func (c *sumSamplesTotalAggrConfig) getValue(s any) aggrValue {
	if c.scrapeTimeIntervals {
		// scrape_time_intervals is refused together with enable_windows, the only
		// source of shared state.
		return &sumSamplesTotalAtValue{}
	}
	if shared, ok := s.(*sumSamplesTotalShared); ok {
		return &sumSamplesTotalAggrValue{shared: shared}
	}
	o := &sumSamplesTotalOwned{}
	o.v.shared = &o.shared
	return &o.v
}

// sumSamplesTotalAtValue is sum_samples_total under scrape_time_intervals. It
// writes the total as of the end of each interval it has samples of, in order,
// so an increment lands in the interval it was scraped in even when the
// aggregator processes it minutes later. A sample for the interval written
// last -- the tail of a round crossing the boundary -- is written into it again,
// at the same timestamp, with the bigger total, which VM's deduplication keeps;
// anything for an interval before that goes to the first interval not written,
// as a total written before one written already would look like a reset.
//
// It holds its state inline, 64 bytes in all: it is never shared, and written is
// a lastWrittenAt other than 0, as no total is written at the Unix epoch. One of
// these is kept for every output series, millions of them.
type sumSamplesTotalAtValue struct {
	pending bySampleTime
	sh      stTotal
}

// stTotal is what sumSamplesTotalAtValue keeps of what it wrote.
type stTotal struct {
	total         float64
	lastWritten   float64
	lastWrittenAt int64 // 0: nothing written yet
}

func (av *sumSamplesTotalAtValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	av.pending.add(sample.timestamp, sample.value)
}

func (av *sumSamplesTotalAtValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	sh := &av.sh
	st := &ctx.st
	labelsLen, samplesLen, started := 0, 0, false
	wrote := false
	write := func(at int64, v float64) {
		at = min(at, ctx.now)
		if !started {
			labelsLen, samplesLen = ctx.beginSeries(key, "sum_samples_total")
			started = true
		}
		ctx.appendSample(at, v)
		sh.lastWritten, sh.lastWrittenAt = v, at
		wrote = true
	}
	slots := av.pending.take(ctx.slots[:0])
	defer func() { ctx.slots = slots[:0] }()
	var late float64
	lateAny := false
	for i := range slots {
		x := &slots[i]
		switch {
		case x.ts == st.written && st.tailOK && x.sum >= 0 && (sh.lastWrittenAt == 0 || sh.lastWrittenAt <= x.ts && sh.total+x.sum >= sh.lastWritten):
			// the tail of the interval written last: write it again, with it
			sh.total += x.sum
			write(x.ts, sh.total)
			x.ts = 0
		case x.ts <= st.written:
			late, lateAny = late+x.sum, true
			x.ts = 0
		}
	}
	if lateAny {
		slots = append(slots, slot{st.written + ctx.a.interval.Milliseconds(), late})
	}
	slices.SortFunc(slots, func(a, b slot) int { return cmp.Compare(a.ts, b.ts) })
	for _, x := range mergeSlots(slots) {
		if x.ts > st.closing {
			av.pending.add(x.ts, x.sum)
			continue
		}
		sh.total += x.sum
		if (sh.lastWrittenAt != 0) && sh.total == sh.lastWritten {
			continue
		}
		write(x.ts, sh.total)
	}
	if !wrote && (sh.lastWrittenAt != 0) {
		at := min(st.closing, ctx.now)
		hb := ctx.a.outputHeartbeatInterval.Milliseconds()
		if at > sh.lastWrittenAt && (hb <= 0 || at-sh.lastWrittenAt >= hb) {
			write(at, sh.total)
		} else {
			ctx.a.skippedOutputs.Inc()
		}
	}
	if started {
		ctx.endSeries(labelsLen, samplesLen)
	}
	if math.Abs(sh.total) >= (1 << 53) {
		// It is time to reset the entry, since it starts losing float64 precision.
		// A tail after this is smaller than what was written, and so goes to the
		// next interval instead, see above.
		sh.total = 0
	}
}

// state is nil: scrape_time_intervals is refused with enable_windows, the only user of shared state.
func (av *sumSamplesTotalAtValue) state() any { return nil }

func (av *sumSamplesTotalAtValue) firstPending() (int64, bool) { return av.pending.first() }

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*sumSamplesAggrConfig) needsInputKey() bool { return false }

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*sumSamplesTotalAggrConfig) needsInputKey() bool { return false }
