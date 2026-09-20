package streamaggr

type avgAggrValue struct {
	sum   float64
	count float64
}

func (av *avgAggrValue) pushSample(_ aggrConfig, sample *pushSample, _ string, _ int64) {
	av.sum += sample.value
	av.count++
}

func (av *avgAggrValue) flush(_ aggrConfig, ctx *flushCtx, key string, _ bool) {
	if av.count > 0 {
		avg := av.sum / av.count
		ctx.appendSeries(key, "avg", avg)
		av.sum = 0
		av.count = 0
	}
}

func (*avgAggrValue) state() any {
	return nil
}

func newAvgAggrConfig() aggrConfig {
	return &avgAggrConfig{}
}

type avgAggrConfig struct{}

func (*avgAggrConfig) getValue(_ any) aggrValue {
	return &avgAggrValue{}
}

// needsInputKey reports that this output looks only at the sample, so the key
// need not carry the labels the aggregation groups away -- compressing them
// costs a hash and two string compares per label per sample, for nothing.
func (*avgAggrConfig) needsInputKey() bool { return false }
