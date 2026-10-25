package remotewrite

import "testing"

// sample_timestamps is on unless turned off: not set, the flag is true for every
// -remoteWrite.url; set once, it applies to all of them; set per url, to each.
func TestSampleTimestampsOnByDefault(t *testing.T) {
	saved := *streamAggrSampleTimestamps
	defer func() { *streamAggrSampleTimestamps = saved }()
	*streamAggrSampleTimestamps = nil
	if !sampleTimestampsFor(0) || !sampleTimestampsFor(3) {
		t.Fatal("not set: want true")
	}
	if err := streamAggrSampleTimestamps.Set("false"); err != nil {
		t.Fatal(err)
	}
	if sampleTimestampsFor(0) || sampleTimestampsFor(2) {
		t.Fatal("set once to false: want false for every url")
	}
	*streamAggrSampleTimestamps = nil
	_ = streamAggrSampleTimestamps.Set("true,false")
	if !sampleTimestampsFor(0) || sampleTimestampsFor(1) {
		t.Fatal("per url: want true, false")
	}
	if !*streamAggrGlobalSampleTimestamps {
		t.Fatal("-streamAggr.sampleTimestamps: want true by default")
	}
}
