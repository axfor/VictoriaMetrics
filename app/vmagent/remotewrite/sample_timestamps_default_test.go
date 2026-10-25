package remotewrite

import (
	"flag"
	"testing"
)

// Not set, sample_timestamps is on with a single -remoteWrite.url and off with
// several, whose storages may not deduplicate; set once, the flag applies to
// every url; set per url, to each.
func TestSampleTimestampsByDefault(t *testing.T) {
	savedURLs, saved := *remoteWriteURLs, *streamAggrSampleTimestamps
	defer func() { *remoteWriteURLs, *streamAggrSampleTimestamps = savedURLs, saved }()

	*streamAggrSampleTimestamps = nil
	*remoteWriteURLs = []string{"http://vm-insert/write"}
	if !sampleTimestampsFor(0) || !globalSampleTimestamps() {
		t.Fatal("not set, one url: want true")
	}
	*remoteWriteURLs = []string{"http://vm-insert/write", "http://other/write"}
	if sampleTimestampsFor(0) || sampleTimestampsFor(1) || globalSampleTimestamps() {
		t.Fatal("not set, two urls: want false")
	}
	if err := streamAggrSampleTimestamps.Set("true"); err != nil {
		t.Fatal(err)
	}
	if !sampleTimestampsFor(0) || !sampleTimestampsFor(1) {
		t.Fatal("set once to true, two urls: want true for every url")
	}
	*streamAggrSampleTimestamps = nil
	_ = streamAggrSampleTimestamps.Set("true,false")
	if !sampleTimestampsFor(0) || sampleTimestampsFor(1) {
		t.Fatal("per url: want true, false")
	}
	*remoteWriteURLs = []string{"http://vm-insert/write"}
	*streamAggrSampleTimestamps = nil
	_ = streamAggrSampleTimestamps.Set("false")
	if sampleTimestampsFor(0) {
		t.Fatal("set to false, one url: want false")
	}

	savedGlobal := *streamAggrGlobalSampleTimestamps
	defer func() {
		_ = flag.Set("streamAggr.sampleTimestamps", "false")
		*streamAggrGlobalSampleTimestamps = savedGlobal
	}()
	if err := flag.Set("streamAggr.sampleTimestamps", "false"); err != nil {
		t.Fatal(err)
	}
	if globalSampleTimestamps() {
		t.Fatal("-streamAggr.sampleTimestamps=false, one url: want false")
	}
}
