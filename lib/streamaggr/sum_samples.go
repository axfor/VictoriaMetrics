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
	ctx.appendSeries(key, "sum_samples_total", total)
}

func (av *sumSamplesTotalAggrValue) state() any {
	return av.shared
}

func newSumSamplesTotalAggrConfig() aggrConfig {
	return &sumSamplesTotalAggrConfig{}
}

type sumSamplesTotalAggrConfig struct{}

func (*sumSamplesTotalAggrConfig) getValue(s any) aggrValue {
	shared, ok := s.(*sumSamplesTotalShared)
	if !ok {
		shared = &sumSamplesTotalShared{}
	}
	return &sumSamplesTotalAggrValue{shared: shared}
}
