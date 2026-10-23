package promscrape

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/chunkedbuffer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// The client sends the acknowledgement it is given and returns the round the
// response names; without one it sends no header.
func TestClientReadDeltaData(t *testing.T) {
	var gotAck []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAck = r.Header.Values(deltaAckHeader)
		w.Header().Set(deltaRoundHeader, "s1:7")
		fmt.Fprintln(w, "foo 1")
	}))
	defer srv.Close()
	c, err := newClient(context.Background(), &ScrapeWork{
		ScrapeURL:          srv.URL,
		ScrapeTimeout:      5 * time.Second,
		AuthConfig:         newTestAuthConfig(t, false, nil),
		ProxyAuthConfig:    newTestAuthConfig(t, false, nil),
		MaxScrapeSize:      16000,
		DisableCompression: true,
	})
	if err != nil {
		t.Fatalf("cannot create client: %s", err)
	}
	var cb chunkedbuffer.Buffer
	_, round, err := c.ReadDeltaData(&cb, "s1:6")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if round != "s1:7" || len(gotAck) != 1 || gotAck[0] != "s1:6" {
		t.Fatalf("round %q, ack sent %q; want round s1:7 and ack s1:6", round, gotAck)
	}
	cb.Reset()
	if _, err := c.ReadData(&cb); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(gotAck) != 0 {
		t.Fatalf("ReadData sent ack %q; want none", gotAck)
	}
}

// What a scrape acknowledges: AckNew before a target has named a round, then the
// last round processed in full; a scrape that fails leaves the last one
// processed, or "<session>:0" when none of the session was. A look at the
// target's response sends AckPeek and changes nothing.
func TestScrapeWorkDeltaAck(t *testing.T) {
	var sw scrapeWork
	sw.Config = &ScrapeWork{ScrapeTimeout: 42 * time.Second, NoStaleMarkers: true}
	sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {}
	var sent []string
	nextRound, nextBody, nextErr := "", "foo 1\n", error(nil)
	sw.ReadDeltaData = func(dst *chunkedbuffer.Buffer, ack string) (string, string, error) {
		sent = append(sent, ack)
		if nextErr != nil {
			return "", nextRound, nextErr
		}
		_, _ = dst.Write([]byte(nextBody))
		return "", nextRound, nil
	}
	scrape := func(round, body string, err error) {
		t.Helper()
		nextRound, nextBody, nextErr = round, body, err
		tsmGlobal.Register(&sw)
		_ = sw.scrapeInternal(123000, 123000)
		tsmGlobal.Unregister(&sw)
	}

	scrape("", "foo 1\n", nil)                   // a target that names no rounds
	scrape("s1:1", "foo 1\n", nil)               // processed
	scrape("s1:2", "foo 1\n", fmt.Errorf("eof")) // fetched, not processed
	scrape("s1:3", "foo{\n", nil)                // not even parsed: still processed, as the scrape is up
	if _, err := sw.getTargetResponse(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	scrape("s2:1", "foo 1\n", fmt.Errorf("eof")) // the target restarted, and this failed
	scrape("s2:2", "foo 1\n", nil)
	scrape("s2:3", "foo 1\n", nil)

	want := []string{deltaAckNew, deltaAckNew, "s1:1", "s1:1", deltaAckPeek, "s1:3", "s2:0", "s2:2"}
	if fmt.Sprint(sent) != fmt.Sprint(want) {
		t.Fatalf("acknowledged\n%q\nwant\n%q", sent, want)
	}
}

// With ReadDeltaData unset, as in a vmagent run with -promscrape.deltaAck=false,
// scraping goes through ReadData and acknowledges nothing.
func TestScrapeWorkWithoutDeltaAck(t *testing.T) {
	var sw scrapeWork
	sw.Config = &ScrapeWork{ScrapeTimeout: 42 * time.Second, NoStaleMarkers: true}
	sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {}
	calls := 0
	sw.ReadData = func(dst *chunkedbuffer.Buffer) (string, error) {
		calls++
		_, _ = dst.Write([]byte("foo 1\n"))
		return "", nil
	}
	tsmGlobal.Register(&sw)
	if err := sw.scrapeInternal(123000, 123000); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tsmGlobal.Unregister(&sw)
	if calls != 1 || sw.deltaAck.Load() != nil {
		t.Fatalf("ReadData called %d times, ack %v; want once and no ack", calls, sw.deltaAck.Load())
	}
}
