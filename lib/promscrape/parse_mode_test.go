package promscrape

import (
	"testing"
)

// Whether the uncompressed response is ever held in memory is decided per
// scrape, from the config and the response size. Three options turn it off and
// none of them says so: no_stale_markers has to be set, and neither
// sample_limit nor series_limit may be. Getting it wrong does not fail a
// scrape, it just costs memory proportional to the response -- on a 30k-key
// sidecar that was 26.94GiB against 4.03GiB -- so the choice is pinned here and
// exported as vm_promscrape_scrapes_by_parse_mode_total.
func TestCanStreamWithoutBodyGates(t *testing.T) {
	const big = 8 << 20 // well past minResponseSizeForStreamParse

	for _, tc := range []struct {
		name string
		cfg  ScrapeWork
		want bool
	}{
		{"the recommended config", ScrapeWork{NoStaleMarkers: true}, true},
		{"no_stale_markers left at its default", ScrapeWork{}, false},
		{"sample_limit set", ScrapeWork{NoStaleMarkers: true, SampleLimit: 1e6}, false},
		{"series_limit set", ScrapeWork{NoStaleMarkers: true, SeriesLimit: 1e6}, false},
		{"stream_parse forced on, sample_limit still off", ScrapeWork{NoStaleMarkers: true, StreamParse: true}, true},
		{"stream_parse forced on but series_limit set", ScrapeWork{NoStaleMarkers: true, StreamParse: true, SeriesLimit: 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sw := &scrapeWork{Config: &tc.cfg}
			sw.prevBodyLen = big
			if got := sw.canStreamWithoutBody(big); got != tc.want {
				t.Errorf("canStreamWithoutBody = %v, want %v -- the uncompressed response %s be held in memory",
					got, tc.want, map[bool]string{true: "would not", false: "would"}[got])
			}
		})
	}
}
