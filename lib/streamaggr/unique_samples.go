package streamaggr

type uniqueSamplesAggrValue struct {
	samples map[float64]struct{}
}

func (av *uniqueSamplesAggrValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	if _, ok := av.samples[sample.value]; !ok {
		av.samples[sample.value] = struct{}{}
	}
}

func (av *uniqueSamplesAggrValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	if len(av.samples) > 0 {
		ctx.appendSeries(key, "unique_samples", float64(len(av.samples)))
		clear(av.samples)
	}
}

func (*uniqueSamplesAggrValue) state() any {
	return nil
}

func newUniqueSamplesAggrConfig() aggrConfig {
	return &uniqueSamplesAggrConfig{}
}

type uniqueSamplesAggrConfig struct{}

func (*uniqueSamplesAggrConfig) getValue(_ any) aggrValue {
	return &uniqueSamplesAggrValue{
		samples: make(map[float64]struct{}),
	}
}

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*uniqueSamplesAggrConfig) needsInputKey() bool { return false }
