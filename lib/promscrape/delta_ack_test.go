package promscrape

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/chunkedbuffer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
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
	sw.Config = &ScrapeWork{ScrapeURL: "http://delta-ack-test-1/metrics", ScrapeTimeout: 42 * time.Second, NoStaleMarkers: true}
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
	sw.Config = &ScrapeWork{ScrapeURL: "http://delta-ack-test-2/metrics", ScrapeTimeout: 42 * time.Second, NoStaleMarkers: true}
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
	if calls != 1 || sw.deltaAck != nil {
		t.Fatalf("ReadData called %d times, ack state %v; want once and none", calls, sw.deltaAck)
	}
}

// A scrapeWork is replaced whenever its scrape config or target labels change,
// as editing sample_limit does. The new one goes on acknowledging what the old
// one processed, rather than AckNew, which the target answers by committing the
// round the old one did not process, with every round rolled into it.
func TestScrapeWorkDeltaAckSurvivesReplacement(t *testing.T) {
	const url = "http://delta-ack-test-3/metrics"
	newWork := func(sampleLimit int) (*scrapeWork, *[]string) {
		sw := &scrapeWork{Config: &ScrapeWork{ScrapeURL: url, ScrapeTimeout: 42 * time.Second, NoStaleMarkers: true, SampleLimit: sampleLimit}}
		sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {}
		sent := &[]string{}
		round := 0
		sw.ReadDeltaData = func(dst *chunkedbuffer.Buffer, ack string) (string, string, error) {
			*sent = append(*sent, ack)
			round++
			_, _ = dst.Write([]byte("foo 1\nbar 2\nbaz 3\n"))
			return "", fmt.Sprintf("s1:%d%d", sampleLimit, round), nil
		}
		return sw, sent
	}
	scrape := func(sw *scrapeWork) {
		tsmGlobal.Register(sw)
		_ = sw.scrapeInternal(123000, 123000)
		tsmGlobal.Unregister(sw)
	}
	old, oldSent := newWork(0)
	scrape(old) // processed: round s1:01
	limited, limitedSent := newWork(1)
	scrape(limited) // replaced with sample_limit: 1; rejected
	scrape(limited) // rejected again
	freed, freedSent := newWork(0)
	scrape(freed) // replaced again with the limit gone
	got := fmt.Sprint(*oldSent, *limitedSent, *freedSent)
	want := fmt.Sprint([]string{deltaAckNew}, []string{"s1:01", "s1:01"}, []string{"s1:01"})
	if got != want {
		t.Fatalf("acknowledged %s; want %s", got, want)
	}
}

// States not used for an hour are dropped, the others kept.
func TestPruneDeltaAcks(t *testing.T) {
	idle := deltaAckOf("http://delta-ack-test-4/metrics")
	idle.used.Store(1000)
	busy := deltaAckOf("http://delta-ack-test-5/metrics")
	busy.used.Store(1000 + deltaAckIdle)
	pruneDeltaAcks(1001 + deltaAckIdle)
	if _, ok := deltaAcks.Load("http://delta-ack-test-4/metrics"); ok {
		t.Fatalf("a state idle for over an hour was kept")
	}
	if _, ok := deltaAcks.Load("http://delta-ack-test-5/metrics"); !ok {
		t.Fatalf("a state used within the hour was dropped")
	}
}

// A delta round that stream parsing would fail partway through, after pushing
// some of its blocks, is not pushed at all: it is not acknowledged, so the target
// sends it again with the next round, and the blocks pushed would count twice.
func TestScrapeWorkDeltaRoundPushedWholeOrNot(t *testing.T) {
	protoparserutil.StartUnmarshalWorkers()
	defer protoparserutil.StopUnmarshalWorkers()
	const rows = 200000 // about 4MB: many blocks
	var sb strings.Builder
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&sb, "tokens_total{key=\"k%d\"} 1\n", i)
	}
	plain := []byte(sb.String())
	damaged := encoding.CompressZSTDLevel(nil, plain, 1)
	damaged[len(damaged)*3/4] ^= 0xff // past the first blocks

	f := func(name string, cfg ScrapeWork, body []byte, encoding string, wantPushed int64) {
		t.Helper()
		var sw scrapeWork
		cfg.ScrapeURL = "http://delta-whole-" + name + "/metrics"
		cfg.ScrapeTimeout = 42 * time.Second
		cfg.StreamParse = true
		sw.Config = &cfg
		var pushed atomic.Int64
		sw.PushData = func(_ *auth.Token, wr *prompb.WriteRequest) {
			for _, ts := range wr.Timeseries {
				for _, l := range ts.Labels {
					if l.Name == "__name__" && l.Value == "tokens_total" {
						pushed.Add(int64(len(ts.Samples)))
					}
				}
			}
		}
		sw.ReadDeltaData = func(dst *chunkedbuffer.Buffer, _ string) (string, string, error) {
			_, _ = dst.Write(body)
			return encoding, "s1:1", nil
		}
		tsmGlobal.Register(&sw)
		err := sw.scrapeInternal(123000, 123000)
		tsmGlobal.Unregister(&sw)
		if got := pushed.Load(); got != wantPushed {
			t.Errorf("%s: pushed %d samples of the round (scrape error: %v); want %d", name, got, err, wantPushed)
		}
		if wantPushed == 0 && err == nil {
			t.Errorf("%s: the scrape succeeded; want it failed, and so the round not acknowledged", name)
		}
	}
	// read and parsed at once, without the uncompressed body: no stale markers
	f("limit-streamed", ScrapeWork{NoStaleMarkers: true, SampleLimit: rows - 1}, plain, "", 0)
	f("damaged-streamed", ScrapeWork{NoStaleMarkers: true}, damaged, "zstd", 0)
	// the uncompressed body kept for stale markers
	f("limit-body", ScrapeWork{SampleLimit: rows - 1}, plain, "", 0)
	// and a round that fits goes out whole
	f("fits", ScrapeWork{NoStaleMarkers: true, SampleLimit: rows}, plain, "", rows)
}

// The last acknowledgement goes only to a target that said it takes one, with
// the round and the final header, and the target's 204 is taken as done.
func TestClientFinalDeltaAck(t *testing.T) {
	var features bool
	var finals []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(deltaFinalHeader) != "" {
			finals = append(finals, r.Header.Get(deltaAckHeader))
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set(deltaRoundHeader, "s1:7")
		if features {
			w.Header().Set(deltaFeaturesHeader, "final")
		}
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
	if _, _, err := c.ReadDeltaData(&cb, deltaAckNew); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if sent, err := c.FinalDeltaAck("s1:7", time.Second); sent || err != nil || len(finals) != 0 {
		t.Fatalf("sent %v, error %v, finals %q to a target that does not take one; want nothing sent", sent, err, finals)
	}
	features = true
	cb.Reset()
	if _, _, err := c.ReadDeltaData(&cb, "s1:7"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if sent, err := c.FinalDeltaAck("s1:7", time.Second); !sent || err != nil || fmt.Sprint(finals) != "[s1:7]" {
		t.Fatalf("sent %v, error %v, finals %q; want s1:7 sent and taken", sent, err, finals)
	}
}

// Stopping, every target that had rounds gets the last round processed, through
// the client that last scraped it; one that had none, or no client, gets nothing.
func TestSendFinalDeltaAcks(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	final := func(url string) func(string, time.Duration) (bool, error) {
		return func(ack string, _ time.Duration) (bool, error) {
			mu.Lock()
			defer mu.Unlock()
			got[url] = ack
			return true, nil
		}
	}
	processed := deltaAckOf("http://delta-final-1/metrics")
	ack := "s1:3"
	processed.ack.Store(&ack)
	f1 := final("1")
	processed.final.Store(&f1)
	noRound := deltaAckOf("http://delta-final-2/metrics")
	f2 := final("2")
	noRound.final.Store(&f2)
	noClient := deltaAckOf("http://delta-final-3/metrics")
	noClient.ack.Store(&ack)
	sendFinalDeltaAcks(time.Second)
	if fmt.Sprint(got) != "map[1:s1:3]" {
		t.Fatalf("last acknowledgements sent %v; want only s1:3 to the target that had rounds", got)
	}
	for _, u := range []string{"http://delta-final-1/metrics", "http://delta-final-2/metrics", "http://delta-final-3/metrics"} {
		deltaAcks.Delete(u)
	}
}

// A scrape registers the client that sends its target's last acknowledgement.
func TestScrapeWorkRegistersFinalDeltaAck(t *testing.T) {
	var sw scrapeWork
	sw.Config = &ScrapeWork{ScrapeURL: "http://delta-final-4/metrics", ScrapeTimeout: 42 * time.Second, NoStaleMarkers: true}
	sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {}
	sw.ReadDeltaData = func(dst *chunkedbuffer.Buffer, _ string) (string, string, error) {
		_, _ = dst.Write([]byte("foo 1\n"))
		return "", "s1:1", nil
	}
	var sentAck string
	sw.FinalDeltaAck = func(ack string, _ time.Duration) (bool, error) { sentAck = ack; return true, nil }
	tsmGlobal.Register(&sw)
	_ = sw.scrapeInternal(123000, 123000)
	tsmGlobal.Unregister(&sw)
	defer deltaAcks.Delete("http://delta-final-4/metrics")
	fn := sw.deltaAckState().final.Load()
	if fn == nil {
		t.Fatalf("the scrape did not register its client for the last acknowledgement")
	}
	if _, err := (*fn)("s1:1", time.Second); err != nil || sentAck != "s1:1" {
		t.Fatalf("the registered client sent %q, error %v", sentAck, err)
	}
}
