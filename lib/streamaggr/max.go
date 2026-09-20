package streamaggr

type maxAggrValue struct {
	max     float64
	defined bool
}

func (av *maxAggrValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	if sample.value > av.max || !av.defined {
		av.max = sample.value
	}
	if !av.defined {
		av.defined = true
	}
}

func (av *maxAggrValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	if av.defined {
		ctx.appendSeries(key, "max", av.max)
		av.max = 0
		av.defined = false
	}
}

func (*maxAggrValue) state() any {
	return nil
}

func newMaxAggrConfig() aggrConfig {
	return &maxAggrConfig{}
}

type maxAggrConfig struct{}

func (*maxAggrConfig) getValue(_ any) aggrValue {
	return &maxAggrValue{}
}

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*maxAggrConfig) needsInputKey() bool { return false }
