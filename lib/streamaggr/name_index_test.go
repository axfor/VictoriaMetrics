package streamaggr

import (
	"bytes"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
)

// nameIndexTestConfig mixes aggregators that can be indexed by metric name with
// ones that cannot, several aggregators sharing a name, and names repeated in
// one match.
const nameIndexTestConfig = `
- match: '{env!=""}'
  interval: 1m
  without: [instance]
  outputs: [sum_samples]
- match: foo
  interval: 1m
  by: [job]
  outputs: [count_samples, sum_samples]
- match: '{__name__="foo",job!="skip"}'
  interval: 1m
  without: [instance]
  outputs: [sum_samples]
- match: '{__name__="bar" or __name__="baz" or __name__="bar"}'
  interval: 1m
  by: [job]
  outputs: [sum_samples]
- match: ['{__name__="baz"}', '{__name__="qux",job="a"}']
  interval: 1m
  drop_input_labels: [env]
  outputs: [count_series]
- match: ['qux', '{env="y"}']
  interval: 1m
  by: [env]
  outputs: [sum_samples]
- match: '{__name__=~"ba.*"}'
  interval: 1m
  by: [job]
  outputs: [sum_samples]
- match: foo
  interval: 1m
  input_relabel_configs:
  - target_label: job
    replacement: relabeled
  by: [job]
  outputs: [sum_samples]
- match: '{__name__="other",job="none"}'
  interval: 1m
  outputs: [count_samples]
- match: zed
  interval: 1m
  by: [job]
  outputs: [count_samples]
`

// nameIndexTestSeries returns series with and without names, some matching
// aggregators by name only, of the same name both in a run and apart, so that
// aggregators get them both in place and copied.
func nameIndexTestSeries(round int) []prompb.TimeSeries {
	var b strings.Builder
	for i, s := range []string{
		`foo{job="a",instance="1",env="x"}`,
		`foo{job="skip",instance="2",env="x"}`,
		`bar{job="a",instance="1"}`,
		`baz{job="b",instance="1",env="y"}`,
		`foo{job="b",instance="3"}`,
		`qux{job="a",instance="1"}`,
		`qux{job="b",instance="1"}`,
		`other{job="a"}`,
		`bar{job="b",instance="2",env="z"}`,
		`other{job="b"}`,
		`zed{job="a"}`,
		`zed{job="b"}`,
	} {
		fmt.Fprintf(&b, "%s %d\n", s, (i+1)*round)
	}
	tss := prometheus.MustParsePromMetrics(b.String(), time.Now().UnixMilli())
	return slices.Insert(tss, 9, prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "job", Value: "a"}, {Name: "env", Value: "x"}},
		Samples: []prompb.Sample{{Value: float64(round), Timestamp: time.Now().UnixMilli()}},
	})
}

// nameIndexRun is what aggregators wrote, the matchIdxs of every push and the
// samples lag they recorded.
type nameIndexRun struct {
	output     []string
	matchIdxs  [][]byte
	samplesLag string
}

// pushWithAndWithoutNameIndex pushes the same rounds to aggregators loaded from
// config with and without the name index.
func pushWithAndWithoutNameIndex(t *testing.T, config string) (indexed, plain nameIndexRun) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		collect := func(dst *[]string) PushFunc {
			return func(tss []prompb.TimeSeries) {
				mu.Lock()
				defer mu.Unlock()
				for _, ts := range tss {
					*dst = append(*dst, fmt.Sprintf("%s %s %v", getMetricName(ts.Labels), promLabelsString(ts.Labels), ts.Samples))
				}
			}
		}
		withIndex, err := LoadFromData([]byte(config), collect(&indexed.output), nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		withoutIndex, err := LoadFromData([]byte(config), collect(&plain.output), nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		if withIndex.byName == nil {
			t.Fatalf("the config is not indexed by name")
		}
		withoutIndex.byName, withoutIndex.indexed, withoutIndex.unindexed = nil, nil, nil

		for round := 1; round <= 3; round++ {
			time.Sleep(50 * time.Second)
			// The second batch has foo only, so the other aggregators are spared it.
			for _, tss := range [][]prompb.TimeSeries{nameIndexTestSeries(round), nameIndexTestSeries(round)[:2]} {
				indexed.matchIdxs = append(indexed.matchIdxs, slices.Clone(withIndex.Push(tss, nil)))
				plain.matchIdxs = append(plain.matchIdxs, slices.Clone(withoutIndex.Push(tss, nil)))
			}
		}
		time.Sleep(2 * time.Minute)
		indexed.samplesLag = samplesLagMetrics(withIndex)
		plain.samplesLag = samplesLagMetrics(withoutIndex)
		withIndex.MustStop()
		withoutIndex.MustStop()
	})
	slices.Sort(indexed.output)
	slices.Sort(plain.output)
	return indexed, plain
}

// samplesLagMetrics returns the vm_streamaggr_samples_lag_seconds lines of a.
// The outputs label lists the outputs in map order, so they are sorted here.
func samplesLagMetrics(a *Aggregators) string {
	var bb bytes.Buffer
	a.ms.WritePrometheus(&bb)
	var lines []string
	for _, line := range strings.Split(bb.String(), "\n") {
		if !strings.HasPrefix(line, "vm_streamaggr_samples_lag_seconds") {
			continue
		}
		line = outputsLabelRe.ReplaceAllStringFunc(line, func(label string) string {
			outputs := strings.Split(label[len(`outputs="`):len(label)-1], ",")
			slices.Sort(outputs)
			return `outputs="` + strings.Join(outputs, ",") + `"`
		})
		lines = append(lines, line)
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

var outputsLabelRe = regexp.MustCompile(`outputs="[^"]*"`)

func promLabelsString(labels []prompb.Label) string {
	var b strings.Builder
	for _, l := range labels {
		fmt.Fprintf(&b, "%s=%q,", l.Name, l.Value)
	}
	return b.String()
}

// The name index only skips aggregators that could not match: every aggregator
// writes what it writes without the index, and the same series count as matched.
func TestNameIndexChangesNothing(t *testing.T) {
	for _, config := range []string{
		nameIndexTestConfig,
		// One more aggregator without match takes every series.
		nameIndexTestConfig + "- interval: 1m\n  by: [job]\n  outputs: [count_samples]\n",
	} {
		indexed, plain := pushWithAndWithoutNameIndex(t, config)
		names := map[string]bool{}
		for _, s := range plain.output {
			names[s[:strings.Index(s, " ")]] = true
		}
		if len(names) < 7 {
			t.Fatalf("too little output to compare: %d metric names in %q", len(names), plain.output)
		}
		if !slices.Equal(indexed.output, plain.output) {
			t.Fatalf("output differs with the name index\nwith:\n%s\nwithout:\n%s", strings.Join(indexed.output, "\n"), strings.Join(plain.output, "\n"))
		}
		for i := range plain.matchIdxs {
			if !slices.Equal(indexed.matchIdxs[i], plain.matchIdxs[i]) {
				t.Fatalf("push %d: matchIdxs %v with the name index, %v without", i, indexed.matchIdxs[i], plain.matchIdxs[i])
			}
		}
		// Self-monitoring stays the same too: every aggregator records the lag of
		// every push, 0 if nothing in it matched.
		if !strings.Contains(plain.samplesLag, "_count") {
			t.Fatalf("no samples lag recorded: %q", plain.samplesLag)
		}
		if indexed.samplesLag != plain.samplesLag {
			t.Fatalf("samples lag differs with the name index\nwith:\n%s\nwithout:\n%s", indexed.samplesLag, plain.samplesLag)
		}
	}
}

// Scrape loops push concurrently. Pushes of the same series from several
// goroutines at once write what as many sequential pushes write: the values are
// integers, so the order of the additions does not change the sums.
func TestNameIndexConcurrentPush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var concurrent, sequential []string
		collect := func(dst *[]string) PushFunc {
			return func(tss []prompb.TimeSeries) {
				mu.Lock()
				defer mu.Unlock()
				for _, ts := range tss {
					*dst = append(*dst, fmt.Sprintf("%s %s %v", getMetricName(ts.Labels), promLabelsString(ts.Labels), ts.Samples))
				}
			}
		}
		withIndex, err := LoadFromData([]byte(nameIndexTestConfig), collect(&concurrent), nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		withoutIndex, err := LoadFromData([]byte(nameIndexTestConfig), collect(&sequential), nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		withoutIndex.byName, withoutIndex.indexed, withoutIndex.unindexed = nil, nil, nil

		const goroutines = 8
		for round := 1; round <= 3; round++ {
			time.Sleep(50 * time.Second)
			tss := nameIndexTestSeries(round)
			want := slices.Clone(withoutIndex.Push(tss, nil))
			for range goroutines - 1 {
				withoutIndex.Push(tss, nil)
			}
			var wg sync.WaitGroup
			for range goroutines {
				wg.Go(func() {
					if got := withIndex.Push(tss, nil); !slices.Equal(got, want) {
						t.Errorf("matchIdxs %v, want %v", got, want)
					}
				})
			}
			wg.Wait()
		}
		time.Sleep(2 * time.Minute)
		withIndex.MustStop()
		withoutIndex.MustStop()
		slices.Sort(concurrent)
		slices.Sort(sequential)
		if len(sequential) == 0 || !slices.Equal(concurrent, sequential) {
			t.Fatalf("concurrent pushes write\n%s\nsequential ones\n%s", strings.Join(concurrent, "\n"), strings.Join(sequential, "\n"))
		}
	})
}

// On a config reload vmagent stops the old aggregators while pushes to them may
// still be in flight. Such pushes must not panic, nor mark series as aggregated
// by aggregators that will never write them out.
func TestNameIndexPushAfterMustStop(t *testing.T) {
	for _, index := range []bool{false, true} {
		a, err := LoadFromData([]byte(nameIndexTestConfig), func([]prompb.TimeSeries) {}, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		if !index {
			a.byName, a.indexed, a.unindexed = nil, nil, nil
		}
		a.MustStop()
		tss := nameIndexTestSeries(1)
		matchIdxs := a.Push(tss, nil)
		if len(matchIdxs) != len(tss) || slices.Contains(matchIdxs, 1) {
			t.Fatalf("index=%v: matchIdxs %v after MustStop, want %d zeros", index, matchIdxs, len(tss))
		}
	}
}

func TestNameIndexOnlyWhenItPays(t *testing.T) {
	f := func(config string, indexed bool) {
		t.Helper()
		a, err := LoadFromData([]byte(config), func([]prompb.TimeSeries) {}, nil, "test")
		if err != nil {
			t.Fatalf("cannot load config: %s", err)
		}
		defer a.MustStop()
		if (a.byName != nil) != indexed {
			t.Fatalf("indexed=%v, want %v for %s", a.byName != nil, indexed, config)
		}
	}
	rule := func(match string) string {
		return fmt.Sprintf("- match: '%s'\n  interval: 1m\n  outputs: [count_samples]\n", match)
	}
	f(rule(`foo`), false)
	f(rule(`foo`)+rule(`{env!=""}`), false)
	f(rule(`foo`)+rule(`{__name__=~"bar"}`), false)
	f(rule(`foo`)+rule(`bar`), true)
	f(rule(`foo`)+rule(`foo{job="a"}`)+rule(`{env!=""}`), true)
}

func BenchmarkAggregatorsPushNameIndex(b *testing.B) {
	rule := func(match, by string) string {
		return fmt.Sprintf("- match: '%s'\n  interval: 1m\n  by: [%s]\n  outputs: [sum_samples]\n", match, by)
	}
	many := rule(`{env!=""}`, "job") + rule(`{env=""}`, "job")
	for i := range 100 {
		many += rule(fmt.Sprintf("m%d", i%20), []string{"job", "instance", "env", "job, env", "instance, env"}[i/20])
	}
	for _, bc := range []struct {
		name, config string
		names        int // series are named m0, m1 ... and come in runs of 50 per name, or one by one if names < 0
	}{
		{"one-indexed", rule("m0", "job"), 20},
		{"two-same-name", rule("m0", "job") + rule("m0", "instance"), 20},
		{"two-same-name-all-match", rule("m0", "job") + rule("m0", "instance"), 1},
		{"two-names", rule("m0", "job") + rule("m1", "job"), 20},
		{"two-names-interleaved", rule("m0", "job") + rule("m1", "job"), -2},
		{"100-indexed-2-not", many, 20},
	} {
		var tss []prompb.TimeSeries
		for i := range 1000 {
			name := fmt.Sprintf("m%d", i/50%max(bc.names, 1))
			if bc.names < 0 {
				name = fmt.Sprintf("m%d", i%-bc.names)
			}
			tss = append(tss, prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: "__name__", Value: name},
					{Name: "job", Value: fmt.Sprintf("job%d", i%7)},
					{Name: "instance", Value: fmt.Sprintf("instance%d", i%50)},
					{Name: "env", Value: fmt.Sprintf("env%d", i%3)},
				},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1}},
			})
		}
		for _, index := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/index=%v", bc.name, index), func(b *testing.B) {
				a, err := LoadFromData([]byte(bc.config), func([]prompb.TimeSeries) {}, nil, "bench")
				if err != nil {
					b.Fatalf("cannot load config: %s", err)
				}
				defer a.MustStop()
				if !index {
					a.byName, a.indexed, a.unindexed = nil, nil, nil
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(tss)))
				b.RunParallel(func(pb *testing.PB) {
					var matchIdxs []byte
					for pb.Next() {
						matchIdxs = a.Push(tss, matchIdxs)
					}
				})
			})
		}
	}
}
