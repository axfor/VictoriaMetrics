package streamaggr

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
)

type aggrOutputs struct {
	m                  sync.Map
	useSharedState     bool
	useInputKey        bool
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

	again:
		v, ok := ao.m.Load(outputKey)
		if !ok {
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
			v = nv
			outputKey = bytesutil.InternString(outputKey)
			vNew, loaded := ao.m.LoadOrStore(outputKey, v)
			if loaded {
				// Use the entry created by a concurrent goroutine.
				v = vNew
			}
		}
		av := v.(*aggrValues)
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

func (ao *aggrOutputs) flushState(ctx *flushCtx) {
	m := &ao.m
	var outputs []aggrValue
	m.Range(func(k, v any) bool {
		// Atomically delete the entry from the map, so new entry is created for the next flush.
		av := v.(*aggrValues)
		av.mu.Lock()

		// check for stale entries
		deleted := ctx.flushTimestamp > av.deleteDeadline
		if deleted {
			if ao.resetMarkerOnStale {
				ao.appendResetMarkers(ctx, av, k.(string), ctx.flushTimestamp)
			}
			// Mark the current entry as deleted
			av.deleteDeadline = -1
			av.mu.Unlock()
			m.Delete(k)
			return true
		}
		outputKey := k.(string)
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
			m.Delete(k)
		}
		return true
	})
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
