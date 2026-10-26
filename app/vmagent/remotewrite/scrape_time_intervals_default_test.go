package remotewrite

import (
	"flag"
	"testing"
)

// Not set, scrape_time_intervals is on with a single -remoteWrite.url and off with
// several, whose storages may not deduplicate; set once, the flag applies to
// every url; set per url, to each.
func TestScrapeTimeIntervalsByDefault(t *testing.T) {
	savedURLs, saved := *remoteWriteURLs, *streamAggrScrapeTimeIntervals
	defer func() { *remoteWriteURLs, *streamAggrScrapeTimeIntervals = savedURLs, saved }()

	*streamAggrScrapeTimeIntervals = nil
	*remoteWriteURLs = []string{"http://vm-insert/write"}
	if !scrapeTimeIntervalsFor(0) || !globalScrapeTimeIntervals() {
		t.Fatal("not set, one url: want true")
	}
	*remoteWriteURLs = []string{"http://vm-insert/write", "http://other/write"}
	if scrapeTimeIntervalsFor(0) || scrapeTimeIntervalsFor(1) || globalScrapeTimeIntervals() {
		t.Fatal("not set, two urls: want false")
	}
	if err := streamAggrScrapeTimeIntervals.Set("true"); err != nil {
		t.Fatal(err)
	}
	if !scrapeTimeIntervalsFor(0) || !scrapeTimeIntervalsFor(1) {
		t.Fatal("set once to true, two urls: want true for every url")
	}
	*streamAggrScrapeTimeIntervals = nil
	_ = streamAggrScrapeTimeIntervals.Set("true,false")
	if !scrapeTimeIntervalsFor(0) || scrapeTimeIntervalsFor(1) {
		t.Fatal("per url: want true, false")
	}
	*remoteWriteURLs = []string{"http://vm-insert/write"}
	*streamAggrScrapeTimeIntervals = nil
	_ = streamAggrScrapeTimeIntervals.Set("false")
	if scrapeTimeIntervalsFor(0) {
		t.Fatal("set to false, one url: want false")
	}

	savedGlobal := *streamAggrGlobalScrapeTimeIntervals
	defer func() {
		_ = flag.Set("streamAggr.scrapeTimeIntervals", "false")
		*streamAggrGlobalScrapeTimeIntervals = savedGlobal
	}()
	if err := flag.Set("streamAggr.scrapeTimeIntervals", "false"); err != nil {
		t.Fatal(err)
	}
	if globalScrapeTimeIntervals() {
		t.Fatal("-streamAggr.scrapeTimeIntervals=false, one url: want false")
	}
}

// The former names of the flags, -remoteWrite.streamAggr.sampleTimestamps and
// -streamAggr.sampleTimestamps, still work; the new ones take precedence.
func TestScrapeTimeIntervalsFormerFlagNames(t *testing.T) {
	savedURLs, saved, savedOld := *remoteWriteURLs, *streamAggrScrapeTimeIntervals, *streamAggrSampleTimestamps
	defer func() {
		*remoteWriteURLs, *streamAggrScrapeTimeIntervals, *streamAggrSampleTimestamps = savedURLs, saved, savedOld
	}()
	*remoteWriteURLs = []string{"http://vm-insert/write"}
	*streamAggrScrapeTimeIntervals, *streamAggrSampleTimestamps = nil, nil
	_ = streamAggrSampleTimestamps.Set("false")
	if scrapeTimeIntervalsFor(0) {
		t.Fatal("-remoteWrite.streamAggr.sampleTimestamps=false alone: want false")
	}
	_ = streamAggrScrapeTimeIntervals.Set("true")
	if !scrapeTimeIntervalsFor(0) {
		t.Fatal("both set: want the new name's true")
	}

	savedGlobal, savedGlobalOld := *streamAggrGlobalScrapeTimeIntervals, *streamAggrGlobalSampleTimestamps
	defer func() {
		*streamAggrGlobalScrapeTimeIntervals, *streamAggrGlobalSampleTimestamps = savedGlobal, savedGlobalOld
		visitSetFlags = flag.Visit
	}()
	setOnly := func(names ...string) {
		visitSetFlags = func(fn func(*flag.Flag)) {
			for _, n := range names {
				fn(&flag.Flag{Name: n})
			}
		}
	}
	*streamAggrGlobalScrapeTimeIntervals, *streamAggrGlobalSampleTimestamps = false, false
	setOnly()
	if !globalScrapeTimeIntervals() {
		t.Fatal("neither set, one url: want true")
	}
	setOnly("streamAggr.sampleTimestamps")
	if globalScrapeTimeIntervals() {
		t.Fatal("-streamAggr.sampleTimestamps=false alone: want false")
	}
	*streamAggrGlobalScrapeTimeIntervals = true
	setOnly("streamAggr.sampleTimestamps", "streamAggr.scrapeTimeIntervals")
	if !globalScrapeTimeIntervals() {
		t.Fatal("both set: want the new name's true")
	}
}
