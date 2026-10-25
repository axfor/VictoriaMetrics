package streamaggr

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envtemplate"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fscore"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promrelabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/histogram"
	"gopkg.in/yaml.v2"
)

// defines ingested samples lag quantile to determine a time to wait before flush.
// It's not configurable at the moment.
const flushQuantile = 0.95

var supportedOutputs = []string{
	"avg",
	"count_samples",
	"count_series",
	"histogram_bucket",
	"increase",
	"increase_prometheus",
	"last",
	"max",
	"min",
	"quantiles(phi1, ..., phiN)",
	"rate_avg",
	"rate_sum",
	"stddev",
	"stdvar",
	"sum_samples",
	"sum_samples_total",
	"total",
	"total_prometheus",
	"unique_samples",
}

var (
	// lc contains information about all compressed labels for streaming aggregation
	lc promutil.LabelsCompressor

	_ = metrics.NewGauge(`vm_streamaggr_labels_compressor_size_bytes`, func() float64 {
		return float64(lc.SizeBytes())
	})

	_ = metrics.NewGauge(`vm_streamaggr_labels_compressor_items_count`, func() float64 {
		return float64(lc.ItemsCount())
	})
)

// LoadFromFile loads Aggregators from the given path and uses the given pushFunc for pushing the aggregated data.
//
// opts can contain additional options. If opts is nil, then default options are used.
//
// alias is used as url label in metrics exposed for the returned Aggregators.
//
// The returned Aggregators must be stopped with MustStop() when no longer needed.
func LoadFromFile(path string, pushFunc PushFunc, opts *Options, alias string) (*Aggregators, error) {
	data, err := fscore.ReadFileOrHTTP(path)
	if err != nil {
		return nil, fmt.Errorf("cannot load aggregators: %w", err)
	}
	data = envtemplate.ReplaceBytes(data)

	as, err := loadFromData(data, path, pushFunc, opts, alias)
	if err != nil {
		return nil, fmt.Errorf("cannot initialize aggregators from %q: %w; see https://docs.victoriametrics.com/victoriametrics/stream-aggregation/#stream-aggregation-config", path, err)
	}

	return as, nil
}

// Options contains optional settings for the Aggregators.
type Options struct {
	// DedupInterval is deduplication interval for samples received for the same time series.
	//
	// The last sample per each series is left per each DedupInterval if DedupInterval > 0.
	//
	// By default deduplication is disabled.
	//
	// The deduplication can be set up individually per each aggregation via dedup_interval option.
	DedupInterval time.Duration

	// DropInputLabels is an optional list of labels to drop from samples before de-duplication and stream aggregation.
	DropInputLabels []string

	// NoAlignFlushToInterval disables alignment of flushes to the aggregation interval.
	//
	// By default flushes are aligned to aggregation interval.
	//
	// The alignment of flushes can be disabled individually per each aggregation via no_align_flush_to_interval option.
	NoAlignFlushToInterval bool

	// FlushOnShutdown enables flush of incomplete aggregation state on startup and shutdown.
	//
	// By default incomplete state is dropped.
	//
	// The flush of incomplete state can be enabled individually per each aggregation via flush_on_shutdown option.
	FlushOnShutdown bool

	// KeepMetricNames instructs to leave metric names as is for the output time series without adding any suffix.
	//
	// By default the following suffix is added to every output time series:
	//
	//     input_name:<interval>[_by_<by_labels>][_without_<without_labels>]_<output>
	//
	// This option can be overridden individually per each aggregation via keep_metric_names option.
	KeepMetricNames bool

	// IgnoreOldSamples instructs to ignore samples with timestamps older than the current aggregation interval.
	//
	// By default all the samples are taken into account.
	//
	// This option can be overridden individually per each aggregation via ignore_old_samples option.
	IgnoreOldSamples bool

	// IgnoreFirstIntervals sets the number of aggregation intervals to be ignored on start.
	//
	// By default, zero intervals are ignored.
	//
	// This option can be overridden individually per each aggregation via ignore_first_intervals option.
	IgnoreFirstIntervals int

	// KeepInput enables keeping all the input samples after the aggregation.
	//
	// By default, aggregates samples are dropped, while the remaining samples are written to the corresponding -remoteWrite.url.
	KeepInput bool

	// EnableWindows enables aggregation in windows
	EnableWindows bool

	// SampleTimestamps writes sum_samples and sum_samples_total at the end of the
	// interval the input samples' timestamps fall in instead of at the flush, so
	// an input processed late still counts towards the interval it was scraped in.
	//
	// This option can be overridden individually per each aggregation via sample_timestamps option.
	SampleTimestamps bool
}

// Config is a configuration for a single stream aggregation.
type Config struct {
	// Name is an optional name of the Config.
	//
	// It is used as `name` label in the exposed metrics for the given Config.
	Name string `yaml:"name,omitempty"`

	// Match is a label selector for filtering time series for the given selector.
	//
	// If the match isn't set, then all the input time series are processed.
	Match *promrelabel.IfExpression `yaml:"match,omitempty"`

	// Interval is the interval between aggregations.
	Interval string `yaml:"interval"`

	// NoAlignFlushToInterval disables aligning of flushes to multiples of Interval.
	// By default flushes are aligned to Interval.
	//
	// See also FlushOnShutdown.
	NoAlignFlushToInterval *bool `yaml:"no_align_flush_to_interval,omitempty"`

	// FlushOnShutdown defines whether to flush incomplete aggregation state on startup and shutdown.
	// By default incomplete aggregation state is dropped, since it may confuse users.
	FlushOnShutdown *bool `yaml:"flush_on_shutdown,omitempty"`

	// DedupInterval is an optional interval for deduplication.
	DedupInterval string `yaml:"dedup_interval,omitempty"`

	// Staleness interval is interval after which the series state will be reset if no samples have been sent during it.
	// The parameter is only relevant for outputs: total, total_prometheus, increase, increase_prometheus and histogram_bucket.
	StalenessInterval string `yaml:"staleness_interval,omitempty"`

	// IgnoreFirstSampleInterval specifies the interval after which the agent begins sending samples.
	// By default, it is set to the staleness interval, and it helps reduce the initial sample load after an agent restart.
	// This parameter is relevant only for the following outputs: total, total_prometheus, increase, increase_prometheus, and histogram_bucket.
	IgnoreFirstSampleInterval string `yaml:"ignore_first_sample_interval,omitempty"`

	// ResetMarkerOnStale instructs to emit a single sample with zero value for total, total_prometheus and sum_samples_total outputs
	// when their state is dropped because no input samples were received during staleness_interval,
	// and one interval before the first sample of a newly created output state.
	//
	// Without the marker the output restarts from a small value after the state is dropped, and query functions
	// such as increase() cannot detect the counter reset if the new value isn't smaller than the last value before the gap.
	// The zero sample makes the reset explicit. The leading zero also covers state lost without a marker,
	// e.g. on vmagent restart or crash.
	//
	// The marker is correct only if the inputs themselves restart from zero after the gap, e.g. when the scrape targets
	// delete idle series. If inputs stop reporting for longer than staleness_interval without restarting
	// (for example, because of a scrape outage), the marker makes the values before the gap to be counted twice.
	ResetMarkerOnStale *bool `yaml:"reset_marker_on_stale,omitempty"`

	// OutputHeartbeatInterval makes sum_samples_total skip a flush whose value is
	// the same as the one last written, re-sending an unchanged output only this
	// often. With scrape targets reporting increments, most outputs are unchanged
	// in any one interval -- measured on a rotating key set, three quarters of
	// them -- and writing each one again every interval is that much ingestion
	// for a value the storage already holds.
	//
	// Skipping is safe for this output because a change is always written: a gap
	// therefore spans an interval over which the value did not move, so the
	// samples on either side of it are equal and increase() over any window
	// containing the gap is unaffected.
	//
	// The heartbeat exists for a different reason -- a series with no sample for
	// longer than the query side's lookbehind (5 minutes by default in
	// VictoriaMetrics) reads as stale -- so keep this below that, or raise
	// -search.maxStalenessInterval to match. Empty or 0 disables it, and every
	// flush is written as before.
	OutputHeartbeatInterval string `yaml:"output_heartbeat_interval,omitempty"`

	// Outputs is a list of output aggregate functions to produce.
	//
	// The following names are allowed:
	//
	// - avg - the average value across all the samples
	// - count_samples - counts the input samples
	// - count_series - counts the number of unique input series
	// - histogram_bucket - creates VictoriaMetrics histogram for input samples
	// - increase - calculates the increase over input series
	// - increase_prometheus - calculates the increase over input series, ignoring the first sample in new time series
	// - last - the last biggest sample value
	// - max - the maximum sample value
	// - min - the minimum sample value
	// - quantiles(phi1, ..., phiN) - quantiles' estimation for phi in the range [0..1]
	// - rate_avg - calculates average of rate for input counters
	// - rate_sum - calculates sum of rate for input counters
	// - stddev - standard deviation across all the samples
	// - stdvar - standard variance across all the samples
	// - sum_samples - sums the input sample values
	// - total - aggregates input counters
	// - total_prometheus - aggregates input counters, ignoring the first sample in new time series
	// - unique_samples - counts the number of unique sample values
	//
	// The output time series will have the following names by default:
	//
	//   input_name:<interval>[_by_<by_labels>][_without_<without_labels>]_<output>
	//
	// See also KeepMetricNames
	//
	Outputs []string `yaml:"outputs"`

	// KeepMetricNames instructs to leave metric names as is for the output time series without adding any suffix.
	KeepMetricNames *bool `yaml:"keep_metric_names,omitempty"`

	// IgnoreOldSamples instructs to ignore samples with old timestamps outside the current aggregation interval.
	IgnoreOldSamples *bool `yaml:"ignore_old_samples,omitempty"`

	// IgnoreFirstIntervals sets number of aggregation intervals to be ignored on start.
	IgnoreFirstIntervals *int `yaml:"ignore_first_intervals,omitempty"`

	// By is an optional list of labels for grouping input series.
	//
	// See also Without.
	//
	// If neither By nor Without are set, then the Outputs are calculated
	// individually per each input time series.
	By []string `yaml:"by,omitempty"`

	// Without is an optional list of labels, which must be excluded when grouping input series.
	//
	// See also By.
	//
	// If neither By nor Without are set, then the Outputs are calculated
	// individually per each input time series.
	Without []string `yaml:"without,omitempty"`

	// DropInputLabels is an optional list with labels, which must be dropped before further processing of input samples.
	//
	// Labels are dropped before de-duplication and aggregation.
	DropInputLabels *[]string `yaml:"drop_input_labels,omitempty"`

	// InputRelabelConfigs is an optional relabeling rules, which are applied on the input
	// before aggregation.
	InputRelabelConfigs []promrelabel.RelabelConfig `yaml:"input_relabel_configs,omitempty"`

	// OutputRelabelConfigs is an optional relabeling rules, which are applied
	// on the aggregated output before being sent to remote storage.
	OutputRelabelConfigs []promrelabel.RelabelConfig `yaml:"output_relabel_configs,omitempty"`

	// EnableWindows enables aggregation in windows
	EnableWindows *bool `yaml:"enable_windows,omitempty"`

	// SampleTimestamps writes the outputs at the timestamps of the input samples instead of at the flush.
	SampleTimestamps *bool `yaml:"sample_timestamps,omitempty"`
}

// Aggregators aggregates metrics passed to Push and calls pushFunc for aggregated data.
type Aggregators struct {
	as []*aggregator

	// byName maps a metric name to the indexes in as of the aggregators whose match
	// can select only series with this name or a few other names; unindexed holds
	// the rest. Push hands a series only to the aggregators listed for its name
	// and to unindexed. byName is nil if too few aggregators can be indexed.
	byName    map[string][]int
	indexed   []int
	unindexed []*aggregator

	// configData contains marshaled configs.
	// It is used in Equal() for comparing Aggregators.
	configData []byte

	// filePath is the path to config file used for creating the Aggregators.
	filePath string

	// ms contains metrics associated with the Aggregators.
	ms *metrics.Set
}

// FilePath returns path to file with the configuration used for creating the given Aggregators.
func (a *Aggregators) FilePath() string {
	return a.filePath
}

// LoadFromData loads aggregators from data.
//
// opts can contain additional options. If opts is nil, then default options are used.
func LoadFromData(data []byte, pushFunc PushFunc, opts *Options, alias string) (*Aggregators, error) {
	return loadFromData(data, "inmemory", pushFunc, opts, alias)
}

func loadFromData(data []byte, filePath string, pushFunc PushFunc, opts *Options, alias string) (*Aggregators, error) {
	var cfgs []*Config
	if err := yaml.UnmarshalStrict(data, &cfgs); err != nil {
		return nil, fmt.Errorf("cannot parse stream aggregation config: %w", err)
	}

	ms := metrics.NewSet()
	as := make([]*aggregator, len(cfgs))
	late := &stRoundTable{m: map[stRoundKey]int64{}} // shared by the rules of this config, see stPlace
	for i, cfg := range cfgs {
		a, err := newAggregator(cfg, filePath, pushFunc, ms, opts, alias, i+1, late)
		if err != nil {
			// Stop already initialized aggregators before returning the error.
			for _, a := range as[:i] {
				a.MustStop()
			}
			return nil, fmt.Errorf("cannot initialize aggregator #%d: %w", i, err)
		}
		as[i] = a
	}
	configData, err := json.Marshal(cfgs)
	if err != nil {
		logger.Panicf("BUG: cannot marshal the provided configs: %s", err)
	}

	metrics.RegisterSet(ms)
	byName, indexed, unindexed := indexAggregatorsByName(as)
	return &Aggregators{
		as:         as,
		byName:     byName,
		indexed:    indexed,
		unindexed:  unindexed,
		configData: configData,
		filePath:   filePath,
		ms:         ms,
	}, nil
}

// minIndexedAggregators is the number of aggregators with names in their match
// from which indexing them pays for looking up the name of every input series.
const minIndexedAggregators = 2

// indexAggregatorsByName maps metric names to the indexes in as of the aggregators
// whose match can select only series with these names, returns the indexes of
// these aggregators, and the other aggregators in unindexed. It returns nil, nil,
// nil if fewer than minIndexedAggregators can be indexed.
func indexAggregatorsByName(as []*aggregator) (map[string][]int, []int, []*aggregator) {
	byName := make(map[string][]int)
	var indexed []int
	var unindexed []*aggregator
	for i, a := range as {
		names := a.match.MetricNames()
		if len(names) == 0 {
			unindexed = append(unindexed, a)
			continue
		}
		indexed = append(indexed, i)
		for _, name := range names {
			// Names may repeat; an aggregator must get each series only once.
			if idxs := byName[name]; len(idxs) == 0 || idxs[len(idxs)-1] != i {
				byName[name] = append(idxs, i)
			}
		}
	}
	if len(indexed) < minIndexedAggregators {
		return nil, nil, nil
	}
	return byName, indexed, unindexed
}

// IsEnabled returns true if Aggregators has at least one configured aggregator
func (a *Aggregators) IsEnabled() bool {
	if a == nil {
		return false
	}
	if len(a.as) == 0 {
		return false
	}
	return true
}

// MustStop stops a.
func (a *Aggregators) MustStop() {
	if a == nil {
		return
	}

	metrics.UnregisterSet(a.ms, true)
	a.ms = nil

	for _, aggr := range a.as {
		aggr.MustStop()
	}
	a.as = nil
}

// Equal returns true if a and b are initialized from identical configs.
func (a *Aggregators) Equal(b *Aggregators) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return string(a.configData) == string(b.configData)
}

// Push pushes tss to a.
//
// Push sets matchIdxs[idx] to 1 if the corresponding tss[idx] was used in aggregations.
// Otherwise matchIdxs[idx] is set to 0.
//
// Push returns matchIdxs with len equal to len(tss).
// It reuses the matchIdxs if it has enough capacity to hold len(tss) items.
// Otherwise it allocates new matchIdxs.
func (a *Aggregators) Push(tss []prompb.TimeSeries, matchIdxs []byte) []byte {
	matchIdxs = bytesutil.ResizeNoCopyMayOverallocate(matchIdxs, len(tss))
	for i := range matchIdxs {
		matchIdxs[i] = 0
	}
	if a == nil {
		return matchIdxs
	}

	// MustStop sets a.as to nil, possibly while a push is in flight on a config
	// reload: read it once, and push nothing to stopped aggregators.
	as := a.as
	if len(as) == 0 {
		return matchIdxs
	}

	// One reading of the clock for every rule: under sample_timestamps where a
	// round goes depends on when it is processed, and a boundary passing while
	// the push goes from one rule to the next must not send it two ways.
	now := time.Now()
	if a.byName == nil {
		for _, aggr := range as {
			aggr.pushAt(tss, matchIdxs, now)
		}
		return matchIdxs
	}

	for _, aggr := range a.unindexed {
		aggr.pushAt(tss, matchIdxs, now)
	}
	a.pushByName(as, tss, matchIdxs, now)

	return matchIdxs
}

// pushByName pushes every series in tss to the indexed aggregators listed for its
// name only. The other indexed aggregators could not match it.
func (a *Aggregators) pushByName(as []*aggregator, tss []prompb.TimeSeries, matchIdxs []byte, now time.Time) {
	ctx := getNameIndexCtx(len(as))
	defer putNameIndexCtx(ctx)

	// Series of a metric usually come one after another, and a few metrics may take
	// turns, such as the buckets, the sum and the count of a histogram: look a name
	// up once per run, and the names of the last few runs not at all.
	runStart, runName := 0, ""
	var runIdxs []int
	for i := range tss {
		name := getMetricName(tss[i].Labels)
		if name == runName {
			continue
		}
		ctx.addRun(runIdxs, runStart, i)
		runStart, runName, runIdxs = i, name, ctx.recent.lookup(a.byName, name)
	}
	ctx.addRun(runIdxs, runStart, len(tss))

	for _, aggrIdx := range a.indexed {
		runs := ctx.runs[aggrIdx]
		if len(runs) == 0 {
			// An aggregator records the lag of every push it gets, 0 if nothing
			// in it matched. Keep recording it for the pushes it is spared.
			as[aggrIdx].samplesLag.Update(0)
			continue
		}
		if len(runs) == 1 {
			r := runs[0]
			as[aggrIdx].pushAt(tss[r.start:r.end], matchIdxs[r.start:r.end], now)
			continue
		}
		tssSub := ctx.tss[:0]
		for _, r := range runs {
			tssSub = append(tssSub, tss[r.start:r.end]...)
		}
		matchIdxsSub := bytesutil.ResizeNoCopyMayOverallocate(ctx.matchIdxs, len(tssSub))
		clear(matchIdxsSub)
		as[aggrIdx].pushAt(tssSub, matchIdxsSub, now)
		j := 0
		for _, r := range runs {
			for i := r.start; i < r.end; i++ {
				matchIdxs[i] |= matchIdxsSub[j]
				j++
			}
		}
		ctx.tss = tssSub[:0]
		ctx.tssMaxLen = max(ctx.tssMaxLen, len(tssSub))
		ctx.matchIdxs = matchIdxsSub
	}
}

// getMetricName returns the value of the first __name__ label, which is the one
// promrelabel.IfExpression matches metric names against.
func getMetricName(labels []prompb.Label) string {
	for _, label := range labels {
		if label.Name == "__name__" {
			return label.Value
		}
	}
	return ""
}

// seriesRun is the series from start up to end in a pushed batch.
type seriesRun struct {
	start, end int
}

type nameIndexCtx struct {
	// runs[i] holds the series to push to the aggregator #i.
	runs [][]seriesRun

	recent recentNames

	tss       []prompb.TimeSeries
	tssMaxLen int
	matchIdxs []byte
}

// recentNames remembers the aggregators of the last few names looked up, nil for
// names without aggregators. Its zero entries map "" to nil, which is right:
// byName has no "".
type recentNames struct {
	names [4]string
	idxs  [4][]int
	next  int
}

func (rn *recentNames) lookup(byName map[string][]int, name string) []int {
	for i := range rn.names {
		if rn.names[i] == name {
			return rn.idxs[i]
		}
	}
	idxs := byName[name]
	rn.names[rn.next], rn.idxs[rn.next] = name, idxs
	rn.next = (rn.next + 1) % len(rn.names)
	return idxs
}

// addRun adds the series from start up to end to the aggregators with aggrIdxs.
func (ctx *nameIndexCtx) addRun(aggrIdxs []int, start, end int) {
	for _, aggrIdx := range aggrIdxs {
		runs := ctx.runs[aggrIdx]
		if n := len(runs); n > 0 && runs[n-1].end == start {
			runs[n-1].end = end
		} else {
			runs = append(runs, seriesRun{start: start, end: end})
		}
		ctx.runs[aggrIdx] = runs
	}
}

func getNameIndexCtx(aggregators int) *nameIndexCtx {
	v := nameIndexCtxPool.Get()
	if v == nil {
		v = &nameIndexCtx{}
	}
	ctx := v.(*nameIndexCtx)
	ctx.runs = slices.Grow(ctx.runs[:0], aggregators)[:aggregators]
	return ctx
}

func putNameIndexCtx(ctx *nameIndexCtx) {
	for i := range ctx.runs {
		ctx.runs[i] = ctx.runs[i][:0]
	}
	// Do not hold the pushed series and names.
	ctx.recent = recentNames{}
	clear(ctx.tss[:ctx.tssMaxLen])
	ctx.tssMaxLen = 0
	nameIndexCtxPool.Put(ctx)
}

var nameIndexCtxPool sync.Pool

type currentState struct {

	// isGreen defines a state, where data sample with minDeadline < timestamp < maxDeadline will be aggregated
	// and which is expected to be flushed with maxDeadline timestamp
	isGreen bool

	// maxDeadline is a threshold for samples from different states, when enableWindows is set
	maxDeadline int64
}

func (cs *currentState) newState() *currentState {
	return &currentState{
		isGreen:     cs.isGreen,
		maxDeadline: cs.maxDeadline,
	}
}

// aggregator aggregates input series according to the config passed to NewAggregator
type aggregator struct {
	match *promrelabel.IfExpression

	dropInputLabels []string

	inputRelabeling         *promrelabel.ParsedConfigs
	outputRelabeling        *promrelabel.ParsedConfigs
	stalenessInterval       time.Duration
	outputHeartbeatInterval time.Duration

	keepMetricNames  bool
	ignoreOldSamples bool
	enableWindows    bool
	sampleTimestamps bool

	// stWritten is the end of the interval written last under
	// sample_timestamps, and stTailOK whether this process wrote it; see stState.
	stWritten atomic.Int64
	stTailOK  atomic.Bool
	// stRounds is where rounds went, shared with the other rules of the config,
	// see stPlace; stEpoch and stInflight are for the cut a flush takes, see
	// stCut.
	stRounds   *stRoundTable
	stEpoch    atomic.Uint64
	stInflight [2]atomic.Int64

	by                  []string
	without             []string
	aggregateOnlyByTime bool

	// interval is the interval between flushes
	interval time.Duration

	// intervalPhaseMsec is a Unix timestamp on the grid the flushes are aligned
	// to. time.Truncate counts intervals from the zero time, January 1 of year 1,
	// not from the Unix epoch, and the two grids differ for an interval that does
	// not divide a day, such as 7m. sample_timestamps puts samples into intervals
	// on the flushes' grid.
	intervalPhaseMsec int64

	// dedupInterval is optional deduplication interval for incoming samples
	dedupInterval time.Duration

	// da is set to non-nil if input samples must be de-duplicated
	da *dedupAggr

	// cs persists information of state, which is expected to be flushed next
	cs atomic.Pointer[currentState]

	// minDeadline is used for ignoring old samples when ignoreOldSamples or enableWindows is set
	minDeadline atomic.Int64

	// aggrOutputs contains aggregate states for the given outputs
	aggrOutputs *aggrOutputs

	// time to wait after interval end before flush
	flushAfter   *histogram.Fast
	muFlushAfter sync.Mutex

	// suffix contains a suffix, which should be added to aggregate metric names
	//
	// It contains the interval, labels in (by, without), plus output name.
	// For example, foo_bar metric name is transformed to foo_bar:1m_by_job
	// for `interval: 1m`, `by: [job]`
	suffix string

	wg     sync.WaitGroup
	stopCh chan struct{}

	flushDuration *metrics.Histogram
	samplesLag    *metrics.Histogram

	flushTimeouts     *metrics.Counter
	skippedOutputs    *metrics.Counter
	ignoredOldSamples *metrics.Counter
	ignoredNaNSamples *metrics.Counter
	matchedSamples    *metrics.Counter
}

// PushFunc is called by Aggregators when it needs to push its state to metrics storage
type PushFunc func(tss []prompb.TimeSeries)

type aggrPushFunc func([]pushSample, int64, bool)

// newAggregator creates new aggregator for the given cfg, which pushes the aggregate data to pushFunc.
//
// opts can contain additional options. If opts is nil, then default options are used.
//
// The returned aggregator must be stopped when no longer needed by calling MustStop().
func newAggregator(cfg *Config, path string, pushFunc PushFunc, ms *metrics.Set, opts *Options, alias string, aggrID int, late *stRoundTable) (*aggregator, error) {
	// check cfg.Interval
	if cfg.Interval == "" {
		return nil, fmt.Errorf("missing `interval` option")
	}
	interval, err := time.ParseDuration(cfg.Interval)
	if err != nil {
		return nil, fmt.Errorf("cannot parse `interval: %q`: %w", cfg.Interval, err)
	}
	if interval < time.Second {
		return nil, fmt.Errorf("aggregation interval cannot be smaller than 1s; got %s", interval)
	}

	if opts == nil {
		opts = &Options{}
	}

	// check cfg.DedupInterval
	dedupInterval := opts.DedupInterval
	if cfg.DedupInterval != "" {
		di, err := time.ParseDuration(cfg.DedupInterval)
		if err != nil {
			return nil, fmt.Errorf("cannot parse `dedup_interval: %q`: %w", cfg.DedupInterval, err)
		}
		dedupInterval = di
	}
	if dedupInterval > interval {
		return nil, fmt.Errorf("dedup_interval=%s cannot exceed interval=%s", dedupInterval, interval)
	}
	if dedupInterval > 0 && interval%dedupInterval != 0 {
		return nil, fmt.Errorf("interval=%s must be a multiple of dedup_interval=%s", interval, dedupInterval)
	}

	// check cfg.OutputHeartbeatInterval
	var outputHeartbeatInterval time.Duration
	if cfg.OutputHeartbeatInterval != "" {
		outputHeartbeatInterval, err = time.ParseDuration(cfg.OutputHeartbeatInterval)
		if err != nil {
			return nil, fmt.Errorf("cannot parse `output_heartbeat_interval: %q`: %w", cfg.OutputHeartbeatInterval, err)
		}
		if outputHeartbeatInterval < 0 {
			return nil, fmt.Errorf("`output_heartbeat_interval: %q` must be non-negative", cfg.OutputHeartbeatInterval)
		}
		if outputHeartbeatInterval > 0 && outputHeartbeatInterval < interval {
			return nil, fmt.Errorf("`output_heartbeat_interval: %q` cannot be smaller than `interval: %q`: an output is only "+
				"written on flush, so a heartbeat shorter than the interval changes nothing", cfg.OutputHeartbeatInterval, cfg.Interval)
		}
	}

	// check cfg.StalenessInterval
	stalenessInterval := interval * 2
	if cfg.StalenessInterval != "" {
		stalenessInterval, err = time.ParseDuration(cfg.StalenessInterval)
		if err != nil {
			return nil, fmt.Errorf("cannot parse `staleness_interval: %q`: %w", cfg.StalenessInterval, err)
		}
		if stalenessInterval < interval {
			return nil, fmt.Errorf("staleness_interval=%s cannot be smaller than interval=%s", cfg.StalenessInterval, cfg.Interval)
		}
	}

	// check cfg.IgnoreFirstSampleInterval
	// by default, it equals to the staleness interval to have backward compatibility, see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/7116
	ignoreFirstSampleInterval := stalenessInterval
	if cfg.IgnoreFirstSampleInterval != "" {
		ignoreFirstSampleInterval, err = time.ParseDuration(cfg.IgnoreFirstSampleInterval)
		if err != nil {
			return nil, fmt.Errorf("cannot parse `ignore_first_sample_interval: %q`: %w", cfg.IgnoreFirstSampleInterval, err)
		}
	}

	// Check cfg.DropInputLabels
	dropInputLabels := opts.DropInputLabels
	if v := cfg.DropInputLabels; v != nil {
		dropInputLabels = *v
	}

	// initialize input_relabel_configs and output_relabel_configs
	inputRelabeling, err := promrelabel.ParseRelabelConfigs(cfg.InputRelabelConfigs)
	if err != nil {
		return nil, fmt.Errorf("cannot parse input_relabel_configs: %w", err)
	}
	outputRelabeling, err := promrelabel.ParseRelabelConfigs(cfg.OutputRelabelConfigs)
	if err != nil {
		return nil, fmt.Errorf("cannot parse output_relabel_configs: %w", err)
	}

	// check by and without lists
	by := sortAndRemoveDuplicates(cfg.By)
	without := sortAndRemoveDuplicates(cfg.Without)
	if len(by) > 0 && len(without) > 0 {
		return nil, fmt.Errorf("`by: %s` and `without: %s` lists cannot be set simultaneously; see https://docs.victoriametrics.com/victoriametrics/stream-aggregation/", by, without)
	}
	aggregateOnlyByTime := (len(by) == 0 && len(without) == 0)
	if !aggregateOnlyByTime && len(without) == 0 {
		by = addMissingUnderscoreName(by)
	}

	// check cfg.KeepMetricNames
	keepMetricNames := opts.KeepMetricNames
	if v := cfg.KeepMetricNames; v != nil {
		keepMetricNames = *v
	}
	if keepMetricNames {
		if opts.KeepInput {
			return nil, fmt.Errorf("`-streamAggr.keepInput` and `keep_metric_names` options can't be enabled in the same time," +
				"as it may result in time series collision")
		}
		if len(cfg.Outputs) != 1 {
			return nil, fmt.Errorf("`outputs` list must contain only a single entry if `keep_metric_names` is set; got %q; "+
				"see https://docs.victoriametrics.com/victoriametrics/stream-aggregation/#output-metric-names", cfg.Outputs)
		}
		if cfg.Outputs[0] == "histogram_bucket" || strings.HasPrefix(cfg.Outputs[0], "quantiles(") && strings.Contains(cfg.Outputs[0], ",") {
			return nil, fmt.Errorf("`keep_metric_names` cannot be applied to `outputs: %q`, since they can generate multiple time series; "+
				"see https://docs.victoriametrics.com/victoriametrics/stream-aggregation/#output-metric-names", cfg.Outputs)
		}
	}

	// check cfg.IgnoreOldSamples
	ignoreOldSamples := opts.IgnoreOldSamples
	if v := cfg.IgnoreOldSamples; v != nil {
		ignoreOldSamples = *v
	}

	// check cfg.IgnoreFirstIntervals
	ignoreFirstIntervals := opts.IgnoreFirstIntervals
	if v := cfg.IgnoreFirstIntervals; v != nil {
		ignoreFirstIntervals = *v
	}

	// check cfg.
	enableWindows := opts.EnableWindows
	if v := cfg.EnableWindows; v != nil {
		enableWindows = *v
	}

	// The global option applies where it can, so one flag covers a config that
	// also has aggregations it cannot apply to; set on an aggregation, it has to.
	sampleTimestamps := opts.SampleTimestamps && sampleTimestampsApply(cfg.Outputs, enableWindows, dedupInterval, ignoreOldSamples) == nil
	if v := cfg.SampleTimestamps; v != nil {
		sampleTimestamps = *v
		if sampleTimestamps {
			if err := sampleTimestampsApply(cfg.Outputs, enableWindows, dedupInterval, ignoreOldSamples); err != nil {
				return nil, err
			}
		}
	}

	// Initialize common metric labels
	name := cfg.Name
	if name == "" {
		name = "none"
	}

	// initialize aggrOutputs
	if len(cfg.Outputs) == 0 {
		return nil, fmt.Errorf("`outputs` list must contain at least a single entry from the list %s; "+
			"see https://docs.victoriametrics.com/victoriametrics/stream-aggregation/", supportedOutputs)
	}
	useInputKey := dedupInterval <= 0
	useSharedState := enableWindows && useInputKey
	aggrOutputs := &aggrOutputs{
		configs:            make([]aggrConfig, len(cfg.Outputs)),
		useSharedState:     useSharedState,
		useInputKey:        useInputKey,
		buildInputKey:      true,
		resetMarkerOnStale: cfg.ResetMarkerOnStale != nil && *cfg.ResetMarkerOnStale,
	}
	outputsSeen := make(map[string]struct{}, len(cfg.Outputs))
	for i, output := range cfg.Outputs {
		ac, err := newOutputConfig(output, outputsSeen, useSharedState, ignoreFirstSampleInterval, sampleTimestamps)
		if err != nil {
			return nil, err
		}
		aggrOutputs.configs[i] = ac
	}
	// Narrow useInputKey to what the configured outputs actually read. It is
	// the labels the aggregation groups away -- pod and instance, typically --
	// and carrying them costs a hash and two string compares per label per
	// sample to build a key nothing looks at, plus the length in every key the
	// state map holds. Only the outputs that keep state per input series need
	// it; the rest take the sample and nothing else.
	if useInputKey {
		useInputKey = false
		for _, ac := range aggrOutputs.configs {
			if ac.needsInputKey() {
				useInputKey = true
				break
			}
		}
		aggrOutputs.useInputKey = useInputKey
		// Only here may the input part be left out of the key: dedup off, and
		// no configured output reads it.
		aggrOutputs.buildInputKey = useInputKey
	}

	if aggrOutputs.resetMarkerOnStale && !aggrOutputs.hasCumulativeTotal() {
		return nil, fmt.Errorf("`reset_marker_on_stale` requires `total`, `total_prometheus` or `sum_samples_total` output")
	}
	outputsLabels := make([]string, 0, len(outputsSeen))
	for o := range outputsSeen {
		outputsLabels = append(outputsLabels, o)
	}
	metricLabels := fmt.Sprintf(`outputs=%q,name=%q,path=%q,url=%q,position="%d"`, strings.Join(outputsLabels, ","), name, path, alias, aggrID)
	aggrOutputs.outputSamples = ms.NewCounter(fmt.Sprintf(`vm_streamaggr_output_samples_total{%s}`, metricLabels))

	// initialize suffix to add to metric names after aggregation
	suffix := ":" + cfg.Interval
	if labels := removeUnderscoreName(by); len(labels) > 0 {
		suffix += fmt.Sprintf("_by_%s", strings.Join(labels, "_"))
	}
	if labels := removeUnderscoreName(without); len(labels) > 0 {
		suffix += fmt.Sprintf("_without_%s", strings.Join(labels, "_"))
	}
	suffix += "_"

	// initialize the aggregator
	a := &aggregator{
		match: cfg.Match,

		dropInputLabels:  dropInputLabels,
		inputRelabeling:  inputRelabeling,
		outputRelabeling: outputRelabeling,

		keepMetricNames:   keepMetricNames,
		ignoreOldSamples:  ignoreOldSamples,
		enableWindows:     enableWindows,
		sampleTimestamps:  sampleTimestamps,
		stRounds:          late,
		stalenessInterval: stalenessInterval,

		outputHeartbeatInterval: outputHeartbeatInterval,

		by:                  by,
		without:             without,
		aggregateOnlyByTime: aggregateOnlyByTime,

		interval:          interval,
		intervalPhaseMsec: time.UnixMilli(0).Truncate(interval).UnixMilli(),
		dedupInterval:     dedupInterval,

		aggrOutputs: aggrOutputs,

		suffix: suffix,

		stopCh: make(chan struct{}),

		flushDuration: ms.NewHistogram(fmt.Sprintf(`vm_streamaggr_flush_duration_seconds{%s}`, metricLabels)),
		samplesLag:    ms.NewHistogram(fmt.Sprintf(`vm_streamaggr_samples_lag_seconds{%s}`, metricLabels)),

		matchedSamples:    ms.NewCounter(fmt.Sprintf(`vm_streamaggr_matched_samples_total{%s}`, metricLabels)),
		flushTimeouts:     ms.NewCounter(fmt.Sprintf(`vm_streamaggr_flush_timeouts_total{%s}`, metricLabels)),
		skippedOutputs:    ms.NewCounter(fmt.Sprintf(`vm_streamaggr_skipped_unchanged_outputs_total{%s}`, metricLabels)),
		ignoredNaNSamples: ms.NewCounter(fmt.Sprintf(`vm_streamaggr_ignored_samples_total{reason="nan",%s}`, metricLabels)),
		ignoredOldSamples: ms.NewCounter(fmt.Sprintf(`vm_streamaggr_ignored_samples_total{reason="too_old",%s}`, metricLabels)),
	}

	if dedupInterval > 0 {
		a.da = newDedupAggr()
		a.da.flushTimeouts = ms.NewCounter(fmt.Sprintf(`vm_streamaggr_dedup_flush_timeouts_total{%s}`, metricLabels))
		a.da.flushDuration = ms.NewHistogram(fmt.Sprintf(`vm_streamaggr_dedup_flush_duration_seconds{%s}`, metricLabels))

		_ = ms.NewGauge(fmt.Sprintf(`vm_streamaggr_dedup_state_size_bytes{%s}`, metricLabels), func() float64 {
			n := a.da.sizeBytes()
			return float64(n)
		})
		_ = ms.NewGauge(fmt.Sprintf(`vm_streamaggr_dedup_state_items_count{%s}`, metricLabels), func() float64 {
			n := a.da.itemsCount()
			return float64(n)
		})
	}

	alignFlushToInterval := !opts.NoAlignFlushToInterval
	if v := cfg.NoAlignFlushToInterval; v != nil {
		alignFlushToInterval = !*v
	}

	skipIncompleteFlush := !opts.FlushOnShutdown
	if v := cfg.FlushOnShutdown; v != nil {
		skipIncompleteFlush = !*v
	}

	startTime := time.Now()
	minTime := startTime
	if skipIncompleteFlush && alignFlushToInterval {
		minTime = minTime.Truncate(a.interval)
		if !startTime.Equal(minTime) {
			minTime = minTime.Add(interval)
		}
	}
	if enableWindows {
		a.flushAfter = histogram.GetFast()
	}
	a.minDeadline.Store(minTime.UnixMilli())
	// Under sample_timestamps the interval that ended before the start is taken
	// as written: rounds of it go to the first interval this process writes.
	a.stWritten.Store(a.intervalFloor(startTime.UnixMilli()))
	cs := &currentState{}
	if a.dedupInterval > 0 {
		cs.maxDeadline = minTime.Add(a.dedupInterval).UnixMilli()
	} else {
		cs.maxDeadline = minTime.Add(a.interval).UnixMilli()
	}
	a.cs.Store(cs)

	a.wg.Add(1)
	go func() {
		a.runFlusher(pushFunc, alignFlushToInterval, skipIncompleteFlush, ignoreFirstIntervals)
		a.wg.Done()
	}()

	return a, nil
}

// sampleTimestampsApply returns why sample_timestamps cannot apply to an
// aggregation, or nil if it can.
func sampleTimestampsApply(outputs []string, enableWindows bool, dedupInterval time.Duration, ignoreOldSamples bool) error {
	if enableWindows || dedupInterval > 0 {
		return fmt.Errorf("`sample_timestamps` cannot be combined with `enable_windows` or `dedup_interval`, which decide timestamps of their own")
	}
	if ignoreOldSamples {
		return fmt.Errorf("`sample_timestamps` cannot be combined with `ignore_old_samples`, which drops the samples of rounds processed late that it puts into the intervals they were scraped in")
	}
	for _, o := range outputs {
		if o != "sum_samples" && o != "sum_samples_total" {
			return fmt.Errorf("`sample_timestamps` supports only `sum_samples` and `sum_samples_total` outputs; got %q", o)
		}
	}
	return nil
}

func newOutputConfig(output string, outputsSeen map[string]struct{}, useSharedState bool, ignoreFirstSampleInterval time.Duration, sampleTimestamps bool) (aggrConfig, error) {
	// check for duplicated output
	if _, ok := outputsSeen[output]; ok {
		return nil, fmt.Errorf("`outputs` list contains duplicate aggregation function: %s", output)
	}
	outputsSeen[output] = struct{}{}

	if strings.HasPrefix(output, "quantiles(") {
		if !strings.HasSuffix(output, ")") {
			return nil, fmt.Errorf("missing closing brace for `quantiles()` output")
		}
		argsStr := output[len("quantiles(") : len(output)-1]
		if len(argsStr) == 0 {
			return nil, fmt.Errorf("`quantiles()` must contain at least one phi")
		}
		args := strings.Split(argsStr, ",")
		phis := make([]float64, len(args))
		for i, arg := range args {
			arg = strings.TrimSpace(arg)
			phi, err := strconv.ParseFloat(arg, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse phi=%q for quantiles(%s): %w", arg, argsStr, err)
			}
			if phi < 0 || phi > 1 {
				return nil, fmt.Errorf("phi inside quantiles(%s) must be in the range [0..1]; got %v", argsStr, phi)
			}
			phis[i] = phi
		}
		if _, ok := outputsSeen["quantiles"]; ok {
			return nil, fmt.Errorf("`outputs` list contains duplicated `quantiles()` function, please combine multiple phi* like `quantiles(0.5, 0.9)`")
		}
		outputsSeen["quantiles"] = struct{}{}
		return newQuantilesAggrConfig(phis), nil
	}
	ignoreFirstSampleIntervalSecs := uint64(ignoreFirstSampleInterval.Seconds())

	switch output {
	case "avg":
		return newAvgAggrConfig(), nil
	case "count_samples":
		return newCountSamplesAggrConfig(), nil
	case "count_series":
		return newCountSeriesAggrConfig(), nil
	case "histogram_bucket":
		return newHistogramBucketAggrConfig(useSharedState), nil
	case "increase":
		return newTotalAggrConfig(ignoreFirstSampleIntervalSecs, true, true), nil
	case "increase_prometheus":
		return newTotalAggrConfig(ignoreFirstSampleIntervalSecs, true, false), nil
	case "last":
		return newLastAggrConfig(), nil
	case "max":
		return newMaxAggrConfig(), nil
	case "min":
		return newMinAggrConfig(), nil
	case "rate_avg":
		return newRateAggrConfig(true), nil
	case "rate_sum":
		return newRateAggrConfig(false), nil
	case "stddev":
		return newStddevAggrConfig(), nil
	case "stdvar":
		return newStdvarAggrConfig(), nil
	case "sum_samples":
		return newSumSamplesAggrConfig(sampleTimestamps), nil
	case "sum_samples_total":
		return newSumSamplesTotalAggrConfig(sampleTimestamps), nil
	case "total":
		return newTotalAggrConfig(ignoreFirstSampleIntervalSecs, false, true), nil
	case "total_prometheus":
		return newTotalAggrConfig(ignoreFirstSampleIntervalSecs, false, false), nil
	case "unique_samples":
		return newUniqueSamplesAggrConfig(), nil
	default:
		return nil, fmt.Errorf("unsupported output=%q; supported values: %s; see https://docs.victoriametrics.com/victoriametrics/stream-aggregation/", output, supportedOutputs)
	}
}

func (a *aggregator) runFlusher(pushFunc PushFunc, alignFlushToInterval, skipIncompleteFlush bool, ignoreFirstIntervals int) {
	minTime := time.UnixMilli(a.minDeadline.Load())
	flushTime := minTime.Add(a.interval)
	interval := a.interval
	if a.dedupInterval > 0 {
		interval = a.dedupInterval
	}
	alignedSleep := func() {
		dSleep := time.Until(minTime)
		if dSleep <= 0 {
			return
		}
		timer := timerpool.Get(dSleep)
		defer timer.Stop()
		select {
		case <-a.stopCh:
		case <-timer.C:
		}
	}

	tickerWait := func(t *time.Ticker) bool {
		select {
		case <-a.stopCh:
			return false
		case <-t.C:
			return true
		}
	}

	alignedSleep()

	t := time.NewTicker(interval)
	defer t.Stop()

	var fa *histogram.Fast
	for tickerWait(t) {
		pf := pushFunc
		if a.enableWindows {
			// Calculate delay and wait
			a.muFlushAfter.Lock()
			fa, a.flushAfter = a.flushAfter, histogram.GetFast()
			a.muFlushAfter.Unlock()
			delay := time.Duration(fa.Quantile(flushQuantile)) * time.Millisecond
			histogram.PutFast(fa)
			time.Sleep(delay)
		}

		cs := a.cs.Load().newState()
		deadlineTime := time.UnixMilli(cs.maxDeadline)
		a.dedupFlush(deadlineTime, cs)

		if !flushTime.After(deadlineTime) {
			// It is time to flush the aggregated state
			if ignoreFirstIntervals > 0 {
				a.flush(nil, flushTime, cs, false)
				ignoreFirstIntervals--
			} else {
				a.flush(pf, flushTime, cs, false)
			}
			// Always move past the timestamp just used: a tick landing on or slightly before the boundary
			// would otherwise leave flushTime in place, and the next flush would reuse the same timestamp.
			flushTime = flushTime.Add(a.interval)
			for time.Now().After(flushTime) {
				flushTime = flushTime.Add(a.interval)
			}
			if a.dedupInterval <= 0 {
				cs.maxDeadline = flushTime.UnixMilli()
			}
		}
		if a.enableWindows {
			cs.isGreen = !cs.isGreen
		}
		a.cs.Store(cs)
		if alignFlushToInterval {
			select {
			case <-t.C:
			default:
			}
		}
	}

	cs := a.cs.Load()
	var dedupTime time.Time
	if alignFlushToInterval {
		if a.dedupInterval > 0 {
			dedupTime = time.UnixMilli(cs.maxDeadline)
		}
	} else {
		flushTime = time.Now()
		if a.dedupInterval > 0 {
			dedupTime = flushTime
		}
	}

	a.dedupFlush(dedupTime, cs)
	pf := pushFunc
	if skipIncompleteFlush || ignoreFirstIntervals > 0 {
		pf = nil
	}
	if pf != nil && a.aggrOutputs.resetMarkerOnStale {
		if now := time.Now(); flushTime.After(now) {
			// Don't stamp the incomplete flush on shutdown with the next flush boundary, which is in the future:
			// a restarted aggregator emits the leading zero of reset_marker_on_stale at its own start,
			// and increase() would see X → 0 → X for the same series and count X twice.
			flushTime = now
		}
	}
	a.flush(pf, flushTime, cs, true)
}

func (a *aggregator) dedupFlush(dedupTime time.Time, cs *currentState) {
	if a.dedupInterval <= 0 {
		// The de-duplication is disabled.
		return
	}

	a.minDeadline.Store(cs.maxDeadline)
	startTime := time.Now()
	deleteDeadline := dedupTime.Add(a.stalenessInterval)

	a.da.flush(a.aggrOutputs.pushSamples, deleteDeadline.UnixMilli(), cs.isGreen)

	d := time.Since(startTime)
	a.da.flushDuration.Update(d.Seconds())
	if d > a.dedupInterval {
		a.da.flushTimeouts.Inc()
		logger.Warnf("deduplication couldn't be finished in the configured dedup_interval=%s; it took %.03fs; "+
			"possible solutions: increase dedup_interval; use match filter matching smaller number of series; "+
			"reduce samples' ingestion rate to stream aggregation", a.dedupInterval, d.Seconds())
	}
	for time.Now().After(dedupTime) {
		dedupTime = dedupTime.Add(a.dedupInterval)
	}
	cs.maxDeadline = dedupTime.UnixMilli()
}

// flush flushes aggregator state to pushFunc.
//
// If pushFunc is nil, then the aggregator state is just reset.
func (a *aggregator) flush(pushFunc PushFunc, flushTime time.Time, cs *currentState, isLast bool) {
	startTime := time.Now()
	ao := a.aggrOutputs

	ctx := getFlushCtx(a, ao, pushFunc, flushTime.UnixMilli(), isLast)
	if a.dedupInterval <= 0 {
		a.minDeadline.Store(cs.maxDeadline)
		ctx.isGreen = cs.isGreen
	}
	if a.sampleTimestamps {
		a.stCut()
		ctx.st = stState{written: a.stWritten.Load(), tailOK: a.stTailOK.Load(), closing: math.MaxInt64}
		if !isLast {
			// the intervals that have ended by the flush's time -- with flushes not
			// aligned to the interval, the last one ending before it -- and never
			// one the clock has not got to: where rounds go relies on no rule having
			// written an interval before it ended
			ctx.st.closing = max(min(a.intervalFloor(ctx.flushTimestamp), a.intervalFloor(ctx.now)), ctx.st.written)
		}
	}
	ao.flushState(ctx)
	ctx.flushSeries()
	if a.sampleTimestamps && !isLast && ctx.st.closing > ctx.st.written {
		a.stWritten.Store(ctx.st.closing)
		a.stTailOK.Store(true)
		a.stPruneRounds(ctx.now)
	}
	putFlushCtx(ctx)

	d := time.Since(startTime)
	a.flushDuration.Update(d.Seconds())
	if d > a.interval {
		a.flushTimeouts.Inc()
		logger.Warnf("stream aggregation couldn't be finished in the configured interval=%s; it took %.03fs; "+
			"possible solutions: increase interval; use match filter matching smaller number of series; "+
			"reduce samples' ingestion rate to stream aggregation", a.interval, d.Seconds())
	}
}

var flushConcurrencyCh = make(chan struct{}, cgroup.AvailableCPUs())

// MustStop stops the aggregator.
//
// The aggregator stops pushing the aggregated metrics after this call.
func (a *aggregator) MustStop() {
	close(a.stopCh)
	a.wg.Wait()
}

// Push pushes tss to a.
func (a *aggregator) Push(tss []prompb.TimeSeries, matchIdxs []byte) {
	a.pushAt(tss, matchIdxs, time.Now())
}

// pushAt is Push, with now the time the push is processed.
func (a *aggregator) pushAt(tss []prompb.TimeSeries, matchIdxs []byte, now time.Time) {
	ctx := getPushCtx()
	defer putPushCtx(ctx)

	buf := ctx.buf
	labels := &ctx.labels
	inputLabels := &ctx.inputLabels
	outputLabels := &ctx.outputLabels
	nowMsec := now.UnixMilli()
	deleteDeadline := now.Add(a.stalenessInterval)
	deleteDeadlineMsec := deleteDeadline.UnixMilli()

	minDeadline := a.minDeadline.Load()
	useInputKey := a.aggrOutputs.buildInputKey
	dropLabels := a.dropInputLabels
	ignoreOldSamples := a.ignoreOldSamples
	enableWindows := a.enableWindows
	cs := a.cs.Load()

	// Under sample_timestamps a sample is kept under the end of the interval its
	// round goes to, see stPlace, not under its own timestamp. Every target is
	// scraped at an offset of its own, so the exact timestamps would give an
	// output fed by N targets N sums to keep and N samples to write per interval;
	// and however far behind the aggregator is, an output holds only so many sums.
	sampleTimestamps := a.sampleTimestamps
	if sampleTimestamps {
		in := a.stEnter()
		defer in.Add(-1)
	}
	stTs, stAt := int64(0), int64(0) // the round of the last sample: a push is mostly one round

	var maxLagMsec int64
	for idx, ts := range tss {
		if !a.match.Match(ts.Labels) {
			continue
		}
		matchIdxs[idx] = 1

		if len(dropLabels) > 0 {
			labels.Labels = dropSeriesLabels(labels.Labels[:0], ts.Labels, dropLabels)
		} else {
			labels.Labels = append(labels.Labels[:0], ts.Labels...)
		}
		labels.Labels = a.inputRelabeling.Apply(labels.Labels, 0)
		if len(labels.Labels) == 0 {
			// The metric has been deleted by the relabeling
			continue
		}
		labels.Sort()

		inputLabels.Reset()
		outputLabels.Reset()
		if !a.aggregateOnlyByTime {
			inputLabels.Labels, outputLabels.Labels = getInputOutputLabels(inputLabels.Labels, outputLabels.Labels, labels.Labels, a.by, a.without)
		} else {
			outputLabels.Labels = append(outputLabels.Labels, labels.Labels...)
		}

		bufLen := len(buf)
		buf = compressLabels(buf, inputLabels.Labels, outputLabels.Labels, &ctx.lcCache, useInputKey)
		// key remains valid only by the end of this function and can't be reused after
		// do not intern key because number of unique keys could be too high
		key := bytesutil.ToUnsafeString(buf[bufLen:])
		for _, s := range ts.Samples {
			if math.IsNaN(s.Value) {
				// Skip NaN values
				a.ignoredNaNSamples.Inc()
				continue
			}
			if (ignoreOldSamples || enableWindows) && s.Timestamp < minDeadline {
				// Skip old samples outside the current aggregation interval
				a.ignoredOldSamples.Inc()
				continue
			}
			lagMsec := nowMsec - s.Timestamp
			if lagMsec > maxLagMsec {
				maxLagMsec = lagMsec
			}
			timestamp := s.Timestamp
			if sampleTimestamps {
				if stAt == 0 || stTs != s.Timestamp {
					stTs, stAt = s.Timestamp, a.stPlace(s.Timestamp, nowMsec)
				}
				timestamp = stAt
			}
			if enableWindows && s.Timestamp <= cs.maxDeadline == cs.isGreen {
				ctx.green = append(ctx.green, pushSample{
					key:       key,
					value:     s.Value,
					timestamp: timestamp,
				})
			} else {
				ctx.blue = append(ctx.blue, pushSample{
					key:       key,
					value:     s.Value,
					timestamp: timestamp,
				})
			}
		}
	}
	if enableWindows && maxLagMsec > 0 {
		a.muFlushAfter.Lock()
		a.flushAfter.Update(float64(maxLagMsec))
		a.muFlushAfter.Unlock()
	}
	a.samplesLag.Update(float64(maxLagMsec) / 1_000)

	ctx.buf = buf
	if pushHook != nil {
		pushHook(a)
	}

	pushSamples := a.aggrOutputs.pushSamples
	if a.da != nil {
		pushSamples = a.da.pushSamples
	}

	if len(ctx.blue) > 0 {
		a.matchedSamples.Add(len(ctx.blue))
		pushSamples(ctx.blue, deleteDeadlineMsec, false)
	}

	if len(ctx.green) > 0 {
		a.matchedSamples.Add(len(ctx.green))
		pushSamples(ctx.green, deleteDeadlineMsec, true)
	}
}

// pushHook, when set, is called by a push between placing its samples and
// adding them to the outputs: for the tests of stCut only.
var pushHook func(a *aggregator)

// stPlace returns the end of the interval the round scraped at ts goes to, its
// sample being processed at now. A round goes to the interval it was scraped
// in, if that has not ended by the boundary before now, and otherwise to the
// interval it is processed in, which no aggregator can have written yet: a
// timestamp ahead of the clock counts as now.
//
// The interval is decided at the first of the round's samples any rule of the
// config processes, and kept, by the interval's grid and the scrape timestamp
// all samples of a round share, for the rest of the round in every rule: a
// round comes in blocks, and its tokens and its cost may be in different ones,
// processed seconds apart, a boundary between them. A flush writes the interval
// with the round or, for the outputs it had already written by the time a block
// got to them, writes it again with the block at the next flush, as the tail of
// the interval written last. Only a round still being processed after the
// interval it was put in is more than a boundary behind has its later blocks go
// further, to the interval they are processed in.
//
// Nothing here depends on how far a rule's flush has got: rules flush at their
// own pace, and that would send the tokens of a round to one minute and its cost
// to another. See stCut for why a flush cannot take a block decided before it
// for one decided after.
func (a *aggregator) stPlace(ts, now int64) int64 {
	iv := a.interval.Milliseconds()
	floor := a.intervalFloor(now)
	k := stRoundKey{iv, a.intervalPhaseMsec, ts}
	t := a.stRounds
	t.mu.Lock()
	defer t.mu.Unlock()
	if at, ok := t.m[k]; ok && at >= floor {
		return at
	}
	at := max(a.intervalEnd(min(ts, now)), floor+iv)
	if _, ok := t.m[k]; ok || len(t.m) < stRoundsMax {
		t.m[k] = at
	}
	return at
}

// stRoundTable holds where the rounds of a config's rules went, see stPlace.
// It holds the rounds of the last couple of intervals: one per scrape of every
// target. Pushes of samples each with a timestamp of its own could fill it, so
// past stRoundsMax a round is placed by the same rule without being kept, and
// its blocks are not held together.
type stRoundTable struct {
	mu sync.Mutex
	m  map[stRoundKey]int64
}

type stRoundKey struct{ interval, phase, ts int64 }

const stRoundsMax = 1 << 17

// stPruneRounds drops the rounds of the grid of a that are of no more use: the
// interval they were put in ended more than an interval before now.
func (a *aggregator) stPruneRounds(now int64) {
	iv, phase := a.interval.Milliseconds(), a.intervalPhaseMsec
	before := a.intervalFloor(now) - iv
	t := a.stRounds
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, at := range t.m {
		if k.interval == iv && k.phase == phase && at < before {
			delete(t.m, k)
		}
	}
}

// stEnter registers a push for the cut a flush takes, see stCut; the push ends
// it with Add(-1) on what it returns. It never waits.
func (a *aggregator) stEnter() *atomic.Int64 {
	for {
		e := a.stEpoch.Load()
		in := &a.stInflight[e&1]
		in.Add(1)
		if a.stEpoch.Load() == e {
			return in
		}
		in.Add(-1)
	}
}

// stCut waits for the pushes in progress when the flush begins, which take no
// longer than a push does: where their blocks go was decided at a time before
// the flush, and a block landing in an output after the flush has written it
// would be taken for one decided after, as a block of an interval before the
// one written last. Pushes begun meanwhile go ahead without waiting.
func (a *aggregator) stCut() {
	e := a.stEpoch.Add(1) - 1
	for a.stInflight[e&1].Load() != 0 {
		// a push takes milliseconds; sleep rather than spin, which would take a
		// CPU from the pushes it waits for when vmagent is short of them
		time.Sleep(time.Millisecond)
	}
}

// intervalEnd returns the end of the interval ts falls in, on the grid the
// flushes are aligned to. Intervals are [end - interval, end): a round scraped
// right on a boundary, as scrape_align_interval has them, belongs to the
// interval that starts there, and so does every later one of the same target,
// each an interval after the one before.
func (a *aggregator) intervalEnd(ts int64) int64 {
	return a.intervalFloor(ts) + a.interval.Milliseconds()
}

// intervalFloor returns the boundary at or before ts on the flushes' grid.
func (a *aggregator) intervalFloor(ts int64) int64 {
	phase, iv := a.intervalPhaseMsec, a.interval.Milliseconds()
	d := ts - phase
	q := d / iv
	if d < 0 && d%iv != 0 {
		q--
	}
	return phase + q*iv
}

// compressLabels builds a sample's key: the length of the output part, the
// output part, and -- only when some configured output keeps state per input
// series -- the input part. Compressing the input part costs a hash and two
// string compares per label per sample, so it is left out when nothing will
// read it. See aggrConfig.needsInputKey.
func compressLabels(dst []byte, inputLabels, outputLabels []prompb.Label, c *promutil.CompressorCache, useInputKey bool) []byte {
	bb := bbPool.Get()
	bb.B = lc.CompressCached(bb.B, outputLabels, c)
	dst = encoding.MarshalVarUint64(dst, uint64(len(bb.B)))
	dst = append(dst, bb.B...)
	bbPool.Put(bb)
	if useInputKey {
		dst = lc.CompressCached(dst, inputLabels, c)
	}
	return dst
}

func decompressLabels(dst []prompb.Label, key string) []prompb.Label {
	return lc.Decompress(dst, bytesutil.ToUnsafeBytes(key))
}

type pushCtx struct {
	green        []pushSample
	blue         []pushSample
	labels       promutil.Labels
	inputLabels  promutil.Labels
	outputLabels promutil.Labels
	buf          []byte
	lcCache      promutil.CompressorCache
}

func (ctx *pushCtx) reset() {
	ctx.blue = ctx.blue[:0]
	ctx.green = ctx.green[:0]
	ctx.labels.Reset()
	ctx.inputLabels.Reset()
	ctx.outputLabels.Reset()
	ctx.buf = ctx.buf[:0]
}

type pushSample struct {
	// key identifies a sample that belongs to unique series
	// key value can't be reused
	key       string
	value     float64
	timestamp int64
}

func getPushCtx() *pushCtx {
	v := pushCtxPool.Get()
	if v == nil {
		return &pushCtx{}
	}
	return v.(*pushCtx)
}

func putPushCtx(ctx *pushCtx) {
	ctx.reset()
	pushCtxPool.Put(ctx)
}

var pushCtxPool sync.Pool

func getInputOutputLabels(dstInput, dstOutput, labels []prompb.Label, by, without []string) ([]prompb.Label, []prompb.Label) {
	if len(without) > 0 {
		for _, label := range labels {
			if slices.Contains(without, label.Name) {
				dstInput = append(dstInput, label)
			} else {
				dstOutput = append(dstOutput, label)
			}
		}
	} else {
		for _, label := range labels {
			if !slices.Contains(by, label.Name) {
				dstInput = append(dstInput, label)
			} else {
				dstOutput = append(dstOutput, label)
			}
		}
	}
	return dstInput, dstOutput
}

func getFlushCtx(a *aggregator, ao *aggrOutputs, pushFunc PushFunc, flushTimestamp int64, isLast bool) *flushCtx {
	v := flushCtxPool.Get()
	if v == nil {
		v = &flushCtx{}
	}
	ctx := v.(*flushCtx)
	ctx.a = a
	ctx.ao = ao
	ctx.pushFunc = pushFunc
	ctx.flushTimestamp = flushTimestamp
	ctx.isLast = isLast
	ctx.now = time.Now().UnixMilli()
	return ctx
}

func putFlushCtx(ctx *flushCtx) {
	ctx.reset()
	flushCtxPool.Put(ctx)
}

var flushCtxPool sync.Pool

// flushBatchSeries is how many output series a flush hands to its push function
// at a time. Every rule flushes at the same moment, each with a context of its
// own, and the contexts are kept in flushCtxPool between flushes: what one holds
// is what a batch needs, labels and samples for every series of it, and with 102
// rules at 10_000 series each that was 290 MiB, held from one minute to the
// next. Remote write packs the batches into blocks of its own, so a smaller
// batch only means more calls.
const flushBatchSeries = 1_000

type flushCtx struct {
	a              *aggregator
	ao             *aggrOutputs
	pushFunc       PushFunc
	flushTimestamp int64
	isGreen        bool
	isLast         bool

	// now and st are for sample_timestamps: the wall clock no output is stamped
	// after, and where the aggregator is in the intervals, the same for every
	// output; slots is scratch for an output's sums.
	now   int64
	st    stState
	slots []slot

	tss     []prompb.TimeSeries
	labels  []prompb.Label
	samples []prompb.Sample
}

func (ctx *flushCtx) reset() {
	ctx.a = nil
	ctx.ao = nil
	ctx.pushFunc = nil
	ctx.isGreen = false
	ctx.isLast = false
	ctx.flushTimestamp = 0
	ctx.now, ctx.st = 0, stState{}
	ctx.slots = ctx.slots[:0]
	ctx.resetSeries()
}

func (ctx *flushCtx) resetSeries() {
	clear(ctx.tss)
	ctx.tss = ctx.tss[:0]

	clear(ctx.labels)
	ctx.labels = ctx.labels[:0]

	ctx.samples = ctx.samples[:0]
}

func (ctx *flushCtx) flushSeries() {
	defer ctx.resetSeries()

	tss := ctx.tss
	if len(tss) == 0 {
		// nothing to flush
		return
	}

	outputRelabeling := ctx.a.outputRelabeling
	if outputRelabeling == nil {
		// Fast path - push the output metrics.
		if ctx.pushFunc != nil {
			ctx.pushFunc(tss)
			ctx.ao.outputSamples.Add(samplesCount(tss))
		}
		return
	}

	// Slow path - apply output relabeling and then push the output metrics.
	auxLabels := promutil.GetLabels()
	dstLabels := auxLabels.Labels[:0]
	dst := tss[:0]
	for _, ts := range tss {
		dstLabelsLen := len(dstLabels)
		dstLabels = append(dstLabels, ts.Labels...)
		dstLabels = outputRelabeling.Apply(dstLabels, dstLabelsLen)
		if len(dstLabels) == dstLabelsLen {
			// The metric has been deleted by the relabeling
			continue
		}
		ts.Labels = dstLabels[dstLabelsLen:]
		dst = append(dst, ts)
	}
	if ctx.pushFunc != nil {
		ctx.pushFunc(dst)
		ctx.ao.outputSamples.Add(samplesCount(dst))
	}
	auxLabels.Labels = dstLabels
	promutil.PutLabels(auxLabels)
}

func (ctx *flushCtx) appendSeries(key, suffix string, value float64) {
	ctx.appendSeriesAt(key, suffix, ctx.flushTimestamp, value)
}

func (ctx *flushCtx) appendSeriesAt(key, suffix string, timestamp int64, value float64) {
	labelsLen := len(ctx.labels)
	samplesLen := len(ctx.samples)
	ctx.labels = decompressLabels(ctx.labels, key)
	if !ctx.a.keepMetricNames {
		ctx.labels = addMetricSuffix(ctx.labels, labelsLen, ctx.a.suffix, suffix)
	}
	ctx.samples = append(ctx.samples, prompb.Sample{
		Timestamp: timestamp,
		Value:     value,
	})
	ctx.tss = append(ctx.tss, prompb.TimeSeries{
		Labels:  ctx.labels[labelsLen:],
		Samples: ctx.samples[samplesLen:],
	})

	// Limit the maximum length of ctx.tss in order to limit memory usage.
	if len(ctx.tss) >= flushBatchSeries {
		ctx.flushSeries()
	}
}

// samplesCount counts the samples in tss: a series carries several under
// sample_timestamps, one per pending interval, see beginSeries.
func samplesCount(tss []prompb.TimeSeries) int {
	n := 0
	for i := range tss {
		n += len(tss[i].Samples)
	}
	return n
}

// beginSeries starts an output series that takes several samples, added with
// appendSample and closed with endSeries, so that an output writing more than
// one sample per flush -- one per pending interval under sample_timestamps --
// decompresses its labels and goes to pushFunc once rather than per sample.
func (ctx *flushCtx) beginSeries(key, suffix string) (labelsLen, samplesLen int) {
	labelsLen = len(ctx.labels)
	samplesLen = len(ctx.samples)
	ctx.labels = decompressLabels(ctx.labels, key)
	if !ctx.a.keepMetricNames {
		ctx.labels = addMetricSuffix(ctx.labels, labelsLen, ctx.a.suffix, suffix)
	}
	return labelsLen, samplesLen
}

// appendSample adds a sample to the series beginSeries started. Samples have to
// come in timestamp order.
func (ctx *flushCtx) appendSample(timestamp int64, value float64) {
	ctx.samples = append(ctx.samples, prompb.Sample{
		Timestamp: timestamp,
		Value:     value,
	})
}

// endSeries closes the series beginSeries started; one without samples is dropped.
func (ctx *flushCtx) endSeries(labelsLen, samplesLen int) {
	if len(ctx.samples) == samplesLen {
		ctx.labels = ctx.labels[:labelsLen]
		return
	}
	ctx.tss = append(ctx.tss, prompb.TimeSeries{
		Labels:  ctx.labels[labelsLen:],
		Samples: ctx.samples[samplesLen:],
	})
	if len(ctx.tss) >= flushBatchSeries {
		ctx.flushSeries()
	}
}

func (ctx *flushCtx) appendSeriesWithExtraLabel(key, suffix string, value float64, extraName, extraValue string) {
	labelsLen := len(ctx.labels)
	samplesLen := len(ctx.samples)
	ctx.labels = decompressLabels(ctx.labels, key)
	if !ctx.a.keepMetricNames {
		ctx.labels = addMetricSuffix(ctx.labels, labelsLen, ctx.a.suffix, suffix)
	}
	ctx.labels = append(ctx.labels, prompb.Label{
		Name:  extraName,
		Value: extraValue,
	})
	ctx.samples = append(ctx.samples, prompb.Sample{
		Timestamp: ctx.flushTimestamp,
		Value:     value,
	})
	ctx.tss = append(ctx.tss, prompb.TimeSeries{
		Labels:  ctx.labels[labelsLen:],
		Samples: ctx.samples[samplesLen:],
	})

	// Limit the maximum length of ctx.tss in order to limit memory usage.
	if len(ctx.tss) >= flushBatchSeries {
		ctx.flushSeries()
	}
}

func addMetricSuffix(labels []prompb.Label, offset int, firstSuffix, lastSuffix string) []prompb.Label {
	src := labels[offset:]
	for i := range src {
		label := &src[i]
		if label.Name != "__name__" {
			continue
		}
		bb := bbPool.Get()
		bb.B = append(bb.B, label.Value...)
		bb.B = append(bb.B, firstSuffix...)
		bb.B = append(bb.B, lastSuffix...)
		label.Value = bytesutil.InternBytes(bb.B)
		bbPool.Put(bb)
		return labels
	}
	// The __name__ isn't found. Add it
	bb := bbPool.Get()
	bb.B = append(bb.B, firstSuffix...)
	bb.B = append(bb.B, lastSuffix...)
	labelValue := bytesutil.InternBytes(bb.B)
	labels = append(labels, prompb.Label{
		Name:  "__name__",
		Value: labelValue,
	})
	return labels
}

func addMissingUnderscoreName(labels []string) []string {
	result := []string{"__name__"}
	for _, s := range labels {
		if s == "__name__" {
			continue
		}
		result = append(result, s)
	}
	return result
}

func removeUnderscoreName(labels []string) []string {
	var result []string
	for _, s := range labels {
		if s == "__name__" {
			continue
		}
		result = append(result, s)
	}
	return result
}

func sortAndRemoveDuplicates(a []string) []string {
	if len(a) == 0 {
		return nil
	}
	a = append([]string{}, a...)
	sort.Strings(a)
	dst := a[:1]
	for _, v := range a[1:] {
		if v != dst[len(dst)-1] {
			dst = append(dst, v)
		}
	}
	return dst
}

var bbPool bytesutil.ByteBufferPool
