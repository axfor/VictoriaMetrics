package streamaggr

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// Pushes from several goroutines onto overlapping series, with a flush running
// all the while, must neither lose nor repeat a sample: what comes out of every
// flush adds up to what went in. This is the contract the sync.Map gave for free
// and the sharded map has to keep -- two pushers creating the same entry at once
// must end up on one entry, and a flush copying a shard out must not skip or
// double any entry.
func TestAggrOutputsConserveSamplesUnderConcurrency(t *testing.T) {
	const config = `
- match: acg_requests_total
  interval: 1h
  outputs: [sum_samples]
  keep_metric_names: true
`
	var out atomic.Int64 // sum of every flushed value, scaled to an integer
	pushFunc := func(tss []prompb.TimeSeries) {
		for _, ts := range tss {
			for _, s := range ts.Samples {
				out.Add(int64(s.Value))
			}
		}
	}
	as, err := LoadFromData([]byte(config), pushFunc, nil, "test")
	if err != nil {
		t.Fatalf("cannot load config: %s", err)
	}
	a := as.as[0]
	defer as.MustStop()

	const (
		pushers = 8
		rounds  = 300
		keys    = 5000 // few enough that pushers collide on every key
	)
	var in atomic.Int64
	var wg sync.WaitGroup
	for p := range pushers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tss := make([]prompb.TimeSeries, 0, 100)
			var matchIdxs []byte
			for r := range rounds {
				tss = tss[:0]
				for k := range 100 {
					key := (p*7919 + r*104729 + k*31) % keys
					tss = append(tss, prompb.TimeSeries{
						Labels:  []prompb.Label{{Name: "__name__", Value: "acg_requests_total"}, {Name: "apikey_id", Value: fmt.Sprintf("key-%05d", key)}},
						Samples: []prompb.Sample{{Value: 1, Timestamp: time.Now().UnixMilli()}},
					})
				}
				matchIdxs = as.Push(tss, matchIdxs)
				in.Add(int64(len(tss)))
			}
		}()
	}
	stop := make(chan struct{})
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		for {
			select {
			case <-stop:
				return
			default:
				a.flush(pushFunc, time.Now(), &currentState{}, false)
			}
		}
	}()
	wg.Wait()
	close(stop)
	<-flushed
	a.flush(pushFunc, time.Now(), &currentState{}, true)

	if got, want := out.Load(), in.Load(); got != want {
		t.Fatalf("flushed %d samples in total, pushed %d", got, want)
	}
	if n := entriesIn(a.aggrOutputs); n != 0 {
		t.Fatalf("%d entries left after the last flush, want none", n)
	}
}

// An entry that goes stale is dropped from its shard, and a later push for the
// same series builds a new one rather than finding the dropped one.
func TestAggrOutputsDropStaleEntriesAndRebuild(t *testing.T) {
	const config = `
- match: acg_requests_total
  interval: 1h
  staleness_interval: 1h
  outputs: [sum_samples]
  keep_metric_names: true
`
	var got []float64
	pushFunc := func(tss []prompb.TimeSeries) {
		for _, ts := range tss {
			for _, s := range ts.Samples {
				got = append(got, s.Value)
			}
		}
	}
	as, err := LoadFromData([]byte(config), pushFunc, nil, "test")
	if err != nil {
		t.Fatalf("cannot load config: %s", err)
	}
	a := as.as[0]
	defer as.MustStop()

	push := func(v float64) {
		as.Push([]prompb.TimeSeries{{
			Labels:  []prompb.Label{{Name: "__name__", Value: "acg_requests_total"}, {Name: "apikey_id", Value: "k"}},
			Samples: []prompb.Sample{{Value: v, Timestamp: time.Now().UnixMilli()}},
		}}, nil)
	}
	push(3)
	if n := entriesIn(a.aggrOutputs); n != 1 {
		t.Fatalf("%d entries after one push, want 1", n)
	}
	// Well past the staleness deadline: the entry is dropped, not flushed.
	a.flush(pushFunc, time.Now().Add(3*time.Hour), &currentState{}, false)
	if n := entriesIn(a.aggrOutputs); n != 0 {
		t.Fatalf("%d entries after the stale flush, want 0", n)
	}
	push(5)
	a.flush(pushFunc, time.Now(), &currentState{}, true)
	if len(got) != 1 || got[0] != 5 {
		t.Fatalf("flushed %v, want only the push made after the entry was rebuilt: [5]", got)
	}
}

func entriesIn(ao *aggrOutputs) int {
	n := 0
	for i := range ao.shards {
		sh := &ao.shards[i]
		sh.mu.RLock()
		n += len(sh.m)
		sh.mu.RUnlock()
	}
	return n
}

// With shared state every output has a blue and a green value sharing one
// state, and the two halves must not be confused: a push for the green window
// that lands on a blue value is attributed to the wrong window.
func TestAggrValuesKeepBlueAndGreenApart(t *testing.T) {
	for _, n := range []int{1, 2, 3} {
		configs := make([]aggrConfig, n)
		for i := range configs {
			configs[i] = newHistogramBucketAggrConfig(true)
		}
		ao := &aggrOutputs{configs: configs, useSharedState: true}
		av := ao.newAggrValues()
		blue, green := av.outputs(n, false), av.outputs(n, true)
		if len(blue) != n || len(green) != n {
			t.Fatalf("n=%d: %d blue and %d green outputs, want %d of each", n, len(blue), len(green), n)
		}
		for i := range n {
			if blue[i] == green[i] {
				t.Fatalf("n=%d: output %d is the same value in blue and green", n, i)
			}
			if blue[i].state() != green[i].state() {
				t.Fatalf("n=%d: output %d: green does not share blue's state", n, i)
			}
		}
	}
}

// Without shared state there is no green half, as before the entry was
// flattened, and a single output is held in the entry itself.
func TestAggrValuesWithoutSharedState(t *testing.T) {
	one := &aggrOutputs{configs: []aggrConfig{newSumSamplesTotalAggrConfig()}}
	av := one.newAggrValues()
	if av.more != nil || len(av.outputs(1, false)) != 1 || av.outputs(1, true) != nil {
		t.Fatalf("single output: more=%v, blue %d, green %v", av.more != nil, len(av.outputs(1, false)), av.outputs(1, true))
	}
	two := &aggrOutputs{configs: []aggrConfig{newSumSamplesTotalAggrConfig(), newSumSamplesTotalAggrConfig()}}
	av = two.newAggrValues()
	if len(av.outputs(2, false)) != 2 || av.outputs(2, true) != nil {
		t.Fatalf("two outputs: blue %d, green %v", len(av.outputs(2, false)), av.outputs(2, true))
	}
}
