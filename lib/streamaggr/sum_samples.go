package streamaggr

import (
	"math"
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

func newSumSamplesAggrConfig() aggrConfig {
	return &sumSamplesAggrConfig{}
}

type sumSamplesAggrConfig struct{}

func (*sumSamplesAggrConfig) getValue(_ any) aggrValue {
	return &sumSamplesAggrValue{}
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

func newSumSamplesTotalAggrConfig() aggrConfig {
	return &sumSamplesTotalAggrConfig{}
}

type sumSamplesTotalAggrConfig struct{}

// sumSamplesTotalOwned is a value together with the shared state it owns when
// no other value shares it, so the pair costs one allocation instead of two.
type sumSamplesTotalOwned struct {
	v      sumSamplesTotalAggrValue
	shared sumSamplesTotalShared
}

func (*sumSamplesTotalAggrConfig) getValue(s any) aggrValue {
	if shared, ok := s.(*sumSamplesTotalShared); ok {
		return &sumSamplesTotalAggrValue{shared: shared}
	}
	o := &sumSamplesTotalOwned{}
	o.v.shared = &o.shared
	return &o.v
}

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*sumSamplesAggrConfig) needsInputKey() bool { return false }

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*sumSamplesTotalAggrConfig) needsInputKey() bool { return false }
