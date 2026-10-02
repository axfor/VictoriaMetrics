package promscrape

import "testing"

// A scrape that took longer than the interval leaves the next tick waiting.
// Its samples are scraped when the loop gets to it, and have to be stamped then.
func TestTickTimeUsesTheRealTimeForAStaleTick(t *testing.T) {
	const minute = 60_000
	if got := tickTime(minute, minute+50, minute); got != minute {
		t.Fatalf("an on-time tick moved to %d", got)
	}
	if got := tickTime(minute, minute+minute/10, minute); got != minute {
		t.Fatalf("a tick within a tenth of the interval moved to %d", got)
	}
	if got := tickTime(minute, 12*minute, minute); got != 12*minute {
		t.Fatalf("a tick left waiting 11 minutes was stamped %d, not when the scrape ran", got)
	}
}
