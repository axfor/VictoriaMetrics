package streamaggr

import (
	"sort"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
)

// Inputs are increments since the previous delivered scrape; the output is their cumulative sum across flushes and pods.
func TestSumSamplesTotalAccumulates(t *testing.T) {
	pushes := []pushAt{
		{0, `foo{pod="a"} 2`},
		{30 * time.Second, `foo{pod="b"} 3`},
		{70 * time.Second, `foo{pod="a"} 1`},
		{130 * time.Second, `foo{pod="b"} 4`},
	}
	out := runStaleScenario(t, markerConfig("[sum_samples_total]", true, false), pushes, 4*time.Minute, 0)["foo"]
	if n := len(out); n == 0 || out[n-1].value != 10 {
		t.Fatalf("the output must be the cumulative sum 10 of all increments; got %v", out)
	}
	for i := 1; i < len(out); i++ {
		if out[i].value < out[i-1].value {
			t.Fatalf("the output must never decrease without a marker; got %v", out)
		}
	}
}

// A restart loses no increments pushed after it: there is no baseline sample per input to skip.
// With the marker the restarted output starts from an explicit zero, so increase() over both parts is exact.
func TestSumSamplesTotalRestart(t *testing.T) {
	config := func(marker bool) string {
		return markerConfig("[sum_samples_total]", true, marker)
	}
	run := func(marker bool) []markerSample {
		var out []markerSample
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var tss []prompb.TimeSeries
			pushFunc := func(src []prompb.TimeSeries) {
				mu.Lock()
				tss = appendClonedTimeseries(tss, src)
				mu.Unlock()
			}
			start := time.Now()
			push := func(a *Aggregators, s string) {
				a.Push(prometheus.MustParsePromMetrics(s, time.Now().UnixMilli()), nil)
			}
			a, err := LoadFromData([]byte(config(marker)), pushFunc, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			time.Sleep(30 * time.Second)
			push(a, `foo{pod="a"} 3`)
			time.Sleep(time.Minute)
			push(a, `foo{pod="a"} 3`)
			time.Sleep(time.Minute)
			a.MustStop() // restart while the output is alive: 6 so far
			time.Sleep(30 * time.Second)
			b, err := LoadFromData([]byte(config(marker)), pushFunc, nil, "test")
			if err != nil {
				t.Fatalf("cannot load config: %s", err)
			}
			time.Sleep(30 * time.Second)
			push(b, `foo{pod="b"} 2`) // right after the restart
			time.Sleep(9 * time.Minute)
			push(b, `foo{pod="a"} 4`) // after the output expired
			time.Sleep(90 * time.Second)
			b.MustStop()

			mu.Lock()
			defer mu.Unlock()
			for _, ts := range tss {
				for _, s := range ts.Samples {
					out = append(out, markerSample{time.UnixMilli(s.Timestamp).Sub(start), s.Value})
				}
			}
			sort.SliceStable(out, func(i, j int) bool { return out[i].at < out[j].at })
		})
		return out
	}
	with := run(true)
	if got := increasePure(with); got != 12 {
		t.Fatalf("with marker: want increase 12 (6 before the restart, 2 and 4 after); got %v from %v", got, with)
	}
	without := run(false)
	if got := increasePure(without); got >= 12 {
		t.Fatalf("without marker the restarted output isn't detected as a reset, so it must undercount; got %v from %v", got, without)
	}
}
