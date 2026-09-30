package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
)

var directStatusDate = uint64(time.Date(2026, 9, 26, 5, 0, 0, 0, time.UTC).UnixMilli()) / msecPerDay

type directStatusSeries struct {
	accountID, projectID uint32
	name                 string
	tags                 [][2]string
}

func addDirectStatusSeries(t *testing.T, idb *indexDB, ss []directStatusSeries) {
	t.Helper()
	for _, s := range ss {
		var mn MetricName
		mn.AccountID, mn.ProjectID = s.accountID, s.projectID
		mn.MetricGroup = []byte(s.name)
		for _, kv := range s.tags {
			mn.AddTag(kv[0], kv[1])
		}
		mn.sortTags()
		buf := mn.Marshal(nil)
		is := idb.getIndexSearch(s.accountID, s.projectID, noDeadline)
		var genTSID generationTSID
		if !is.getTSIDByMetricName(&genTSID, buf, directStatusDate) {
			generateTSID(&genTSID.TSID, &mn)
			createAllIndexesForMetricName(idb, &mn, &genTSID.TSID, directStatusDate)
		}
		idb.putIndexSearch(is)
	}
}

func directStatusFilters(t *testing.T, accountID, projectID uint32, key, value string, negative, regexp bool) []*TagFilters {
	t.Helper()
	tfs := NewTagFilters(accountID, projectID)
	var k []byte
	if key != "" {
		k = []byte(key)
	}
	if err := tfs.Add(k, []byte(value), negative, regexp); err != nil {
		t.Fatal(err)
	}
	return []*TagFilters{tfs}
}

// bothStatuses returns the status of tfss scanned from the index and collected
// from the metric names, and whether the second was collected so.
func bothStatuses(t *testing.T, idb *indexDB, accountID, projectID uint32, tfss []*TagFilters, date uint64, focus string, topN int, deadline uint64) (scanned, direct *TSDBStatus, fromNames bool, errScan, errDirect error) {
	t.Helper()
	defer SetMaxMetricIDsForDirectTSDBStatus(0)
	SetMaxMetricIDsForDirectTSDBStatus(0)
	scanned, errScan = idb.GetTSDBStatus(nil, accountID, projectID, tfss, date, focus, topN, 1e6, deadline)
	SetMaxMetricIDsForDirectTSDBStatus(1e6)
	qt := querytracer.New(true, "status")
	direct, errDirect = idb.GetTSDBStatus(qt, accountID, projectID, tfss, date, focus, topN, 1e6, deadline)
	qt.Done()
	return scanned, direct, strings.Contains(qt.String(), "from the metric names"), errScan, errDirect
}

func openDirectStatusStorage(t *testing.T) (*Storage, *indexDB) {
	t.Helper()
	path := t.Name()
	fs.MustRemoveDir(path)
	s := MustOpenStorage(path, OpenOptions{})
	t.Cleanup(func() {
		s.MustClose()
		fs.MustRemoveDir(path)
	})
	_, idb := s.getPrevAndCurrIndexDBs()
	return s, idb
}

// TestTSDBStatusFromMetricNamesMatchesTheIndexScan collects the TSDB status of
// random filters over random series, some of them deleted, both by scanning the
// inverted index of the day or the global one and from the metric names of the
// matching series: every field must be the same, ties at the edge of topN
// included. Label names and values have characters the index escapes or sorts
// apart, and some keys look like the ones the index adds for its own use.
func TestTSDBStatusFromMetricNamesMatchesTheIndexScan(t *testing.T) {
	s, idb := openDirectStatusStorage(t)
	r := rand.New(rand.NewSource(1))
	keys := []string{"A", "Z", "_", "__a", "a", "ab", "b", "job", "le", "ümlaut", "x_y", "0", "a b", "\x01k", "k\x00", "\xfeweird"}
	vals := []string{"", "0", "1", "10", "2", "a", "A", "a b", "é", "x", "xy", "~", "x\x00", "\x02y"}
	names := []string{"", "m", "M", "m_total", "a.b.c", "__x", "m\x01"}
	var ss []directStatusSeries
	for i := 0; i < 600; i++ {
		var tags [][2]string
		for _, k := range keys {
			if r.Intn(3) == 0 {
				v := vals[r.Intn(len(vals))]
				if v == "" {
					v = fmt.Sprintf("u%d", i)
				}
				tags = append(tags, [2]string{k, v})
			}
		}
		ss = append(ss, directStatusSeries{1, 2, names[r.Intn(len(names))], tags})
	}
	addDirectStatusSeries(t, idb, ss)
	s.DebugFlush()
	if _, err := s.DeleteSeries(nil, directStatusFilters(t, 1, 2, "b", "x", false, false), 1e6); err != nil {
		t.Fatal(err)
	}
	s.DebugFlush()
	compared := 0
	for iter := 0; iter < 400; iter++ {
		k := keys[r.Intn(len(keys))]
		var tfss []*TagFilters
		switch r.Intn(3) {
		case 0:
			tfss = directStatusFilters(t, 1, 2, k, ".+", false, true)
		case 1:
			tfss = directStatusFilters(t, 1, 2, k, vals[1+r.Intn(len(vals)-1)], false, false)
		case 2:
			tfss = directStatusFilters(t, 1, 2, "", names[1+r.Intn(len(names)-1)], false, false)
		}
		focus := append(keys, "__name__", "")[r.Intn(len(keys)+2)]
		topN := 1 + r.Intn(6)
		for _, date := range []uint64{directStatusDate, globalIndexDate} {
			scanned, direct, fromNames, errScan, errDirect := bothStatuses(t, idb, 1, 2, tfss, date, focus, topN, noDeadline)
			if errScan != nil || errDirect != nil {
				t.Fatalf("iter %d: %v, %v", iter, errScan, errDirect)
			}
			if !scanned.hasEntries() {
				continue
			}
			compared++
			if !fromNames {
				t.Fatalf("iter %d: the status was not collected from the metric names", iter)
			}
			if !reflect.DeepEqual(scanned, direct) {
				t.Fatalf("iter %d date %d focus %q topN %d filters %s:\nscanned %+v\ndirect  %+v", iter, date, focus, topN, tfss, *scanned, *direct)
			}
		}
	}
	if compared < 400 {
		t.Fatalf("only %d statuses had entries", compared)
	}
}

// TestTSDBStatusFromMetricNamesKeepsTenantsApart has tenants with the same
// series, some deleted, on a day and in the global index: the statuses of each
// are the same both ways.
func TestTSDBStatusFromMetricNamesKeepsTenantsApart(t *testing.T) {
	s, idb := openDirectStatusStorage(t)
	tenants := [][2]uint32{{1, 2}, {1, 3}, {2, 2}, {0, 0}}
	var ss []directStatusSeries
	for _, tenant := range tenants {
		for i := 0; i < 40; i++ {
			ss = append(ss, directStatusSeries{tenant[0], tenant[1], fmt.Sprintf("m%d", i%3), [][2]string{
				{"job", fmt.Sprintf("j%d", i%4)}, {"i", fmt.Sprintf("%d", i)}, {"t", fmt.Sprintf("%d", tenant[1])},
			}})
		}
	}
	addDirectStatusSeries(t, idb, ss)
	s.DebugFlush()
	if _, err := s.DeleteSeries(nil, directStatusFilters(t, 1, 2, "job", "j1", false, false), 1e6); err != nil {
		t.Fatal(err)
	}
	s.DebugFlush()
	for _, date := range []uint64{directStatusDate, globalIndexDate} {
		for _, tenant := range tenants {
			for _, topN := range []int{1, 2, 5, 100} {
				tfss := directStatusFilters(t, tenant[0], tenant[1], "job", "j[0-2]", false, true)
				scanned, direct, fromNames, errScan, errDirect := bothStatuses(t, idb, tenant[0], tenant[1], tfss, date, "job", topN, noDeadline)
				want := uint64(30) // j0..j2 of 40 series; j1 of tenant 1:2 is deleted
				if tenant == [2]uint32{1, 2} {
					want = 20
				}
				if errScan != nil || errDirect != nil || !fromNames || scanned.TotalSeries != want || !reflect.DeepEqual(scanned, direct) {
					t.Fatalf("date %d tenant %v topN %d: %v %v %v\nscanned %+v\ndirect  %+v", date, tenant, topN, errScan, errDirect, fromNames, scanned, direct)
				}
			}
		}
	}
}

// TestTSDBStatusIsScannedWhenAMetricNameIsMissing matches a series whose
// per-day rows are there but not its metric name yet: the status is scanned,
// as it would be without the metric names.
func TestTSDBStatusIsScannedWhenAMetricNameIsMissing(t *testing.T) {
	s, idb := openDirectStatusStorage(t)
	addDirectStatusSeries(t, idb, []directStatusSeries{{1, 2, "m", [][2]string{{"job", "a"}}}})
	var mn MetricName
	mn.AccountID, mn.ProjectID = 1, 2
	mn.MetricGroup = []byte("m")
	mn.AddTag("job", "b")
	mn.sortTags()
	var tsid TSID
	generateTSID(&tsid, &mn)
	idb.createPerDayIndexes(directStatusDate, &tsid, &mn)
	s.DebugFlush()
	for _, value := range []string{"m", "b"} {
		key := ""
		if value == "b" {
			key = "job"
		}
		tfss := directStatusFilters(t, 1, 2, key, value, false, false)
		scanned, direct, fromNames, errScan, errDirect := bothStatuses(t, idb, 1, 2, tfss, directStatusDate, "job", 10, noDeadline)
		if errScan != nil || errDirect != nil || fromNames || !reflect.DeepEqual(scanned, direct) {
			t.Fatalf("filter %s: %v %v, from the names %v\nscanned %+v\ndirect  %+v", tfss, errScan, errDirect, fromNames, scanned, direct)
		}
	}
}

// TestTSDBStatusFromMetricNamesStopsAtTheDeadline asks for a status whose
// deadline has passed: it fails, as the scan does, rather than go on.
func TestTSDBStatusFromMetricNamesStopsAtTheDeadline(t *testing.T) {
	s, idb := openDirectStatusStorage(t)
	var ss []directStatusSeries
	for i := 0; i < 100; i++ {
		ss = append(ss, directStatusSeries{1, 2, "m", [][2]string{{"i", fmt.Sprintf("%d", i)}}})
	}
	addDirectStatusSeries(t, idb, ss)
	s.DebugFlush()
	defer SetMaxMetricIDsForDirectTSDBStatus(0)
	SetMaxMetricIDsForDirectTSDBStatus(1e6)
	tfss := directStatusFilters(t, 1, 2, "", "m", false, false)
	is := idb.getIndexSearch(1, 2, noDeadline)
	defer idb.putIndexSearch(is)
	filter, err := is.searchMetricIDsWithFiltersOnDate(nil, tfss, directStatusDate, 1e6)
	if err != nil || filter.Len() != 100 {
		t.Fatalf("%v, %d series", err, filter.Len())
	}
	is.deadline = 1 // long past
	if status, ok, err := is.getTSDBStatusForMetricIDs(nil, filter, "", 10); err == nil || ok || status != nil {
		t.Fatalf("past the deadline: %+v, %v, %v; want an error", status, ok, err)
	}
}

// TestTSDBStatusFromMetricNamesCountsWhatTheScanMiscounts pins the two places
// where the metric names give the right count and the scan does not: a series
// listed twice in the per-day index, and a label whose name has '=' in it.
func TestTSDBStatusFromMetricNamesCountsWhatTheScanMiscounts(t *testing.T) {
	s, idb := openDirectStatusStorage(t)
	var mn MetricName
	mn.AccountID, mn.ProjectID = 1, 2
	mn.MetricGroup = []byte("m")
	mn.AddTag("job", "a")
	mn.sortTags()
	var tsid TSID
	generateTSID(&tsid, &mn)
	createAllIndexesForMetricName(idb, &mn, &tsid, directStatusDate)
	s.DebugFlush()
	idb.createPerDayIndexes(directStatusDate, &tsid, &mn) // listed again, in another part
	addDirectStatusSeries(t, idb, []directStatusSeries{{1, 2, "e", [][2]string{{"a", "x"}}}, {1, 2, "e", [][2]string{{"a=b", "c"}}}})
	s.DebugFlush()
	_, direct, fromNames, _, err := bothStatuses(t, idb, 1, 2, directStatusFilters(t, 1, 2, "", "m", false, false), directStatusDate, "job", 10, noDeadline)
	if err != nil || !fromNames || direct.TotalSeries != 1 {
		t.Errorf("a series listed twice: %+v, %v", direct, err)
	}
	_, direct, fromNames, _, err = bothStatuses(t, idb, 1, 2, directStatusFilters(t, 1, 2, "", "e", false, false), directStatusDate, "a", 10, noDeadline)
	if want := []TopHeapEntry{{Name: "x", Count: 1}}; err != nil || !fromNames || !reflect.DeepEqual(direct.SeriesCountByFocusLabelValue, want) {
		t.Errorf("focus a with a label a=b: %+v, %v; want %v", direct.SeriesCountByFocusLabelValue, err, want)
	}
}
