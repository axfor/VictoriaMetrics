package streamaggr

type lastAggrValue struct {
	last      float64
	timestamp int64
}

func (av *lastAggrValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	if sample.timestamp >= av.timestamp {
		av.last = sample.value
		av.timestamp = sample.timestamp
	}
}

func (av *lastAggrValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	if av.timestamp > 0 {
		ctx.appendSeries(key, "last", av.last)
		av.timestamp = 0
	}
}

func (*lastAggrValue) state() any {
	return nil
}

func newLastAggrConfig() aggrConfig {
	return &lastAggrConfig{}
}

type lastAggrConfig struct{}

func (*lastAggrConfig) getValue(_ any) aggrValue {
	return &lastAggrValue{}
}

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*lastAggrConfig) needsInputKey() bool { return false }
