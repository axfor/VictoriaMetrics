package streamaggr

import (
	"cmp"
	"math"
	"slices"
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

func newSumSamplesAggrConfig(sampleTimestamps bool) aggrConfig {
	return &sumSamplesAggrConfig{sampleTimestamps: sampleTimestamps}
}

type sumSamplesAggrConfig struct {
	sampleTimestamps bool
}

func (c *sumSamplesAggrConfig) getValue(_ any) aggrValue {
	if c.sampleTimestamps {
		return &sumSamplesAtValue{}
	}
	return &sumSamplesAggrValue{}
}

// sumSamplesAtValue is sum_samples under sample_timestamps: every sum goes to
// the interval its samples were scraped in, ending at their timestamp, rather
// than to the one the aggregator happened to process them in.
type sumSamplesAtValue struct {
	pending bySampleTime
}

func (av *sumSamplesAtValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	av.pending.add(sample.timestamp, sample.value)
}

// flush writes a sum arriving after its interval has been written once already
// as one more sample a few milliseconds earlier -- each flush backs off by its
// own sequence number -- so the two don't share a timestamp, which
// deduplication would keep only one of, and sum_over_time() adds them up.
// Backing off keeps the sample within the interval, (ts - interval, ts].
func (av *sumSamplesAtValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	back := ctx.seq % 1000
	av.pending.drain(func(ts int64, sum float64) {
		ctx.appendSeriesAt(key, "sum_samples", min(ts, ctx.now)-back, sum)
	})
}

func (*sumSamplesAtValue) state() any { return nil }

func (av *sumSamplesAtValue) firstPending() (int64, bool) { return av.pending.first() }

// bySampleTime holds the input sums of one output between two flushes, one per
// distinct sample timestamp, for sample_timestamps. Scrapes aligned to the
// interval give every sample of a round the same timestamp, so there is one sum
// almost always, and more only while the aggregator catches up with rounds it
// fell behind on.
type bySampleTime struct {
	ts   int64 // 0: nothing pending
	sum  float64
	more *[]tsSum
}

type tsSum struct {
	ts  int64
	sum float64
}

func (p *bySampleTime) add(ts int64, v float64) {
	switch {
	case p.ts == 0:
		p.ts, p.sum = ts, v
	case p.ts == ts:
		p.sum += v
	default:
		if p.more == nil {
			p.more = &[]tsSum{}
		}
		for i := range *p.more {
			if (*p.more)[i].ts == ts {
				(*p.more)[i].sum += v
				return
			}
		}
		*p.more = append(*p.more, tsSum{ts, v})
	}
}

// first is the earliest pending timestamp.
func (p *bySampleTime) first() (int64, bool) {
	if p.ts == 0 {
		return 0, false
	}
	first := p.ts
	if p.more != nil {
		for _, x := range *p.more {
			first = min(first, x.ts)
		}
	}
	return first, true
}

// drain calls f for every pending sum in timestamp order and empties p.
func (p *bySampleTime) drain(f func(ts int64, sum float64)) {
	if p.ts == 0 {
		return
	}
	if p.more == nil {
		f(p.ts, p.sum)
		p.ts, p.sum = 0, 0
		return
	}
	all := append(*p.more, tsSum{p.ts, p.sum})
	slices.SortFunc(all, func(a, b tsSum) int { return cmp.Compare(a.ts, b.ts) })
	for _, x := range all {
		f(x.ts, x.sum)
	}
	p.ts, p.sum, p.more = 0, 0, nil
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

func newSumSamplesTotalAggrConfig(sampleTimestamps bool) aggrConfig {
	return &sumSamplesTotalAggrConfig{sampleTimestamps: sampleTimestamps}
}

type sumSamplesTotalAggrConfig struct {
	sampleTimestamps bool
}

// sumSamplesTotalOwned is a value together with the shared state it owns when
// no other value shares it, so the pair costs one allocation instead of two.
type sumSamplesTotalOwned struct {
	v      sumSamplesTotalAggrValue
	shared sumSamplesTotalShared
}

func (c *sumSamplesTotalAggrConfig) getValue(s any) aggrValue {
	if c.sampleTimestamps {
		// sample_timestamps is refused together with enable_windows, the only
		// source of shared state.
		o := &sumSamplesTotalAtOwned{}
		o.v.shared = &o.shared
		return &o.v
	}
	if shared, ok := s.(*sumSamplesTotalShared); ok {
		return &sumSamplesTotalAggrValue{shared: shared}
	}
	o := &sumSamplesTotalOwned{}
	o.v.shared = &o.shared
	return &o.v
}

// sumSamplesTotalAtValue is sum_samples_total under sample_timestamps. It
// writes the total as of each scrape round it has samples of, in timestamp
// order, so an increment lands in the interval it was scraped in even when the
// aggregator processes it minutes later. The written timestamps never go back
// -- a counter that did would look reset -- so a round whose samples arrive
// after a later round was written goes to that later one: the error is bounded
// by how far the targets' rounds are apart in processing, not by how far the
// aggregator is behind.
//
// An unchanged total is written again only as the heartbeat, and then at the
// latest sample timestamp the aggregator has seen rather than at the wall
// clock: while it is behind, a heartbeat at the wall clock would push the
// series ahead of the rounds still to come, and they would all land there.
type sumSamplesTotalAtValue struct {
	shared  *sumSamplesTotalShared
	pending bySampleTime
}

type sumSamplesTotalAtOwned struct {
	v      sumSamplesTotalAtValue
	shared sumSamplesTotalShared
}

func (av *sumSamplesTotalAtValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	av.pending.add(sample.timestamp, sample.value)
}

func (av *sumSamplesTotalAtValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	sh := av.shared
	wrote := false
	av.pending.drain(func(ts int64, sum float64) {
		sh.total += sum
		if sh.written && sh.total == sh.lastWritten {
			return
		}
		at := max(min(ts, ctx.now), sh.lastWrittenAt)
		ctx.appendSeriesAt(key, "sum_samples_total", at, sh.total)
		sh.lastWritten, sh.lastWrittenAt, sh.written = sh.total, at, true
		wrote = true
	})
	if !wrote && sh.written {
		at := min(ctx.watermark, ctx.now)
		hb := ctx.a.outputHeartbeatInterval.Milliseconds()
		if at > sh.lastWrittenAt && (hb <= 0 || at-sh.lastWrittenAt >= hb) {
			ctx.appendSeriesAt(key, "sum_samples_total", at, sh.total)
			sh.lastWrittenAt = at
		} else {
			ctx.a.skippedOutputs.Inc()
		}
	}
	if math.Abs(sh.total) >= (1 << 53) {
		// It is time to reset the entry, since it starts losing float64 precision
		sh.total = 0
	}
}

func (av *sumSamplesTotalAtValue) state() any { return av.shared }

func (av *sumSamplesTotalAtValue) firstPending() (int64, bool) { return av.pending.first() }

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*sumSamplesAggrConfig) needsInputKey() bool { return false }

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*sumSamplesTotalAggrConfig) needsInputKey() bool { return false }
