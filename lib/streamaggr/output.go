package streamaggr

import (
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
)

// aggrOutputShards is how many independently locked maps hold the output
// entries. Pushes run from many scrape goroutines at once; 256 keeps two of them
// landing on the same lock rare at the core counts vmagent runs on.
const aggrOutputShards = 256

// aggrOutputShard is one lock and the entries hashed to it.
//
// The entries used to live in a sync.Map. Under the load one 30k-key
// deployment puts on it -- about nine million output series -- a third of
// vmagent's heap was sync.Map's own trie nodes rather than the entries: some 120
// bytes of indirect and entry nodes per series, where a plain map slot costs a
// few dozen.
type aggrOutputShard struct {
	mu sync.RWMutex
	m  map[string]*aggrValues
	_  [64 - 24 - 8]byte // a cache line to itself, so neighbouring locks do not share one
}

type aggrOutputs struct {
	shards         [aggrOutputShards]aggrOutputShard
	useSharedState bool
	useInputKey    bool
	// buildInputKey is whether compressLabels puts the input part into a
	// sample's key. It is not the same question as useInputKey, which is also
	// false when dedup is on -- there the key layout still carries the input
	// part, and the deduplicator keys its own map on it. Conflating the two
	// collapses every input series in an output group onto one sample.
	buildInputKey      bool
	resetMarkerOnStale bool
	configs            []aggrConfig
	outputSamples      *metrics.Counter
}

// hasCumulativeTotal returns true if ao has total, total_prometheus or sum_samples_total output.
func (ao *aggrOutputs) hasCumulativeTotal() bool {
	for _, c := range ao.configs {
		if _, ok := cumulativeSuffix(c); ok {
			return true
		}
	}
	return false
}

// cumulativeSuffix returns the output suffix for outputs producing a cumulative counter.
func cumulativeSuffix(c aggrConfig) (string, bool) {
	switch t := c.(type) {
	case *totalAggrConfig:
		return t.getSuffix(), !t.resetTotalOnFlush
	case *sumSamplesTotalAggrConfig:
		return "sum_samples_total", true
	}
	return "", false
}

func (ao *aggrOutputs) getInputOutputKey(key string) (string, string) {
	src := bytesutil.ToUnsafeBytes(key)
	outputKeyLen, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		logger.Panicf("BUG: cannot unmarshal outputKeyLen from uvarint")
	}
	src = src[nSize:]
	outputKey := src[:outputKeyLen]
	if !ao.useInputKey {
		return key, bytesutil.ToUnsafeString(outputKey)
	}
	inputKey := src[outputKeyLen:]
	return bytesutil.ToUnsafeString(inputKey), bytesutil.ToUnsafeString(outputKey)
}

func (ao *aggrOutputs) pushSamples(samples []pushSample, deleteDeadline int64, isGreen bool) {
	var inputKey, outputKey string
	var sample *pushSample
	var outputs []aggrValue
	var nv *aggrValues
	for i := range samples {
		sample = &samples[i]
		inputKey, outputKey = ao.getInputOutputKey(sample.key)

		sh := ao.shardOf(outputKey)
	again:
		sh.mu.RLock()
		av := sh.m[outputKey]
		sh.mu.RUnlock()
		if av == nil {
			// The entry is missing in the map. Try creating it.
			nv = &aggrValues{
				blue: make([]aggrValue, len(ao.configs)),
			}
			if ao.useSharedState {
				nv.green = make([]aggrValue, len(ao.configs))
			}
			for idx, ac := range ao.configs {
				nv.blue[idx] = ac.getValue(nil)
				if ao.useSharedState {
					nv.green[idx] = ac.getValue(nv.blue[idx].state())
				}
			}
			key := bytesutil.InternString(outputKey)
			sh.mu.Lock()
			if sh.m == nil {
				sh.m = make(map[string]*aggrValues)
			}
			if av = sh.m[key]; av == nil {
				sh.m[key] = nv
				av = nv
			}
			// Otherwise use the entry created by a concurrent goroutine.
			sh.mu.Unlock()
		}
		av.mu.Lock()
		deleted := av.deleteDeadline < 0
		if !deleted {
			if isGreen {
				outputs = av.green
			} else {
				outputs = av.blue
			}
			for idx, o := range outputs {
				o.pushSample(ao.configs[idx], sample, inputKey, deleteDeadline)
			}
			av.deleteDeadline = deleteDeadline
		}
		av.mu.Unlock()
		if deleted {
			// The entry has been deleted by the concurrent call to flush
			// Try obtaining and updating the entry again.
			goto again
		}
	}
}

// shardOf returns the shard holding outputKey.
func (ao *aggrOutputs) shardOf(outputKey string) *aggrOutputShard {
	return &ao.shards[xxhash.Sum64String(outputKey)%aggrOutputShards]
}

// flushEntry is one entry copied out of a shard, so the shard's lock is not held
// while the entry is flushed.
type flushEntry struct {
	key string
	av  *aggrValues
}

var flushEntriesPool sync.Pool

func (ao *aggrOutputs) flushState(ctx *flushCtx) {
	var entries []flushEntry
	if v := flushEntriesPool.Get(); v != nil {
		entries = *v.(*[]flushEntry)
	}
	for i := range ao.shards {
		sh := &ao.shards[i]
		sh.mu.RLock()
		for k, av := range sh.m {
			entries = append(entries, flushEntry{key: k, av: av})
		}
		sh.mu.RUnlock()
		for _, e := range entries {
			ao.flushEntry(ctx, sh, e.key, e.av)
		}
		clear(entries)
		entries = entries[:0]
	}
	flushEntriesPool.Put(&entries)
}

// removeEntry drops key from sh if it still maps to av.
func (sh *aggrOutputShard) removeEntry(key string, av *aggrValues) {
	sh.mu.Lock()
	if sh.m[key] == av {
		delete(sh.m, key)
	}
	sh.mu.Unlock()
}

func (ao *aggrOutputs) flushEntry(ctx *flushCtx, sh *aggrOutputShard, outputKey string, av *aggrValues) {
	var outputs []aggrValue
	{
		av.mu.Lock()

		// check for stale entries
		deleted := ctx.flushTimestamp > av.deleteDeadline
		if deleted {
			if ao.resetMarkerOnStale {
				ao.appendResetMarkers(ctx, av, outputKey, ctx.flushTimestamp)
			}
			// Mark the current entry as deleted, and drop it straight away: a
			// concurrent push that finds it marked goes back to look it up
			// again, and keeps doing so for as long as it is still in the map.
			av.deleteDeadline = -1
			av.mu.Unlock()
			sh.removeEntry(outputKey, av)
			return
		}
		if ao.resetMarkerOnStale && !av.started && ctx.pushFunc != nil {
			// The first visible flush of a new output entry: emit a zero one interval earlier.
			ao.appendResetMarkers(ctx, av, outputKey, ctx.flushTimestamp-ctx.a.interval.Milliseconds())
			av.started = true
		}
		if ctx.isGreen {
			outputs = av.green
		} else {
			outputs = av.blue
		}
		for i, o := range outputs {
			o.flush(ao.configs[i], ctx, outputKey, ctx.isLast)
		}
		av.mu.Unlock()
		if ctx.isLast {
			sh.removeEntry(outputKey, av)
		}
	}
}

// appendResetMarkers emits a zero sample at the given timestamp for every total, total_prometheus and sum_samples_total output of av.
//
// It is called for an entry which is going to be dropped because of staleness, and before the first visible flush
// of a new entry. The latter covers entries lost without a marker, e.g. on vmagent restart or crash:
// the previous output ends at some value X, and the new output restarts from a small value,
// which increase() cannot detect as a counter reset if it isn't smaller than X.
func (ao *aggrOutputs) appendResetMarkers(ctx *flushCtx, av *aggrValues, outputKey string, timestamp int64) {
	outputs := av.blue
	if ctx.isGreen {
		outputs = av.green
	}
	for i := range outputs {
		if suffix, ok := cumulativeSuffix(ao.configs[i]); ok {
			ctx.appendSeriesAt(outputKey, suffix, timestamp, 0)
		}
	}
}

type aggrValues struct {
	mu             sync.Mutex
	blue           []aggrValue
	green          []aggrValue
	deleteDeadline int64
	started        bool // a zero sample has been emitted before the first visible flush (reset_marker_on_stale)
}

type aggrConfig interface {
	getValue(any) aggrValue
	// needsInputKey reports whether this output reads the input part of a
	// sample's key, i.e. whether it keeps state per input series. It is on the
	// interface so that a new output has to answer it, next to the pushSample
	// whose signature is the answer: getting this wrong does not fail, it
	// silently aggregates the wrong thing.
	needsInputKey() bool
}

type aggrValue interface {
	pushSample(aggrConfig, *pushSample, string, int64)
	flush(aggrConfig, *flushCtx, string, bool)
	state() any
}
