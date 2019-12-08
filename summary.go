package querydigest

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jedib0t/go-pretty/table"
	"gonum.org/v1/gonum/stat"
)

type SlowQuerySummary struct {
	RowSample          string
	TotalTime          float64
	TotalLockTime      float64
	TotalQueryCount    int
	TotalRowsSent      int
	TotalRowsExamined  int
	RawInfo            []*SlowQueryInfo
	stats              *slowQueryStats
	queryTimeHistogram Histogram
}

func (s *SlowQuerySummary) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "%s\n", s.RowSample)
	fmt.Fprintf(&b, "  query time(sec): %f\n", s.TotalTime)
	fmt.Fprintf(&b, "total query count: %d\n", s.TotalQueryCount)

	fmt.Fprintf(&b, "%s\n", s.stats)

	fmt.Fprintf(&b, "Query_time distribution:\n%v\n", s.queryTimeHistogram)

	return b.String()
}

func (s *SlowQuerySummary) ComputeStats() {
	queryTimes := make([]float64, 0, len(s.RawInfo))
	lockTimes := make([]float64, 0, len(s.RawInfo))
	rowsSents := make([]float64, 0, len(s.RawInfo))
	rowsExamines := make([]float64, 0, len(s.RawInfo))

	for _, r := range s.RawInfo {
		if r.QueryTime.QueryTime > 0 {
			queryTimes = append(queryTimes, r.QueryTime.QueryTime)
		}
		if r.QueryTime.LockTime > 0 {
			lockTimes = append(lockTimes, r.QueryTime.LockTime)
		}
		if r.QueryTime.RowsSent > 0 {
			rowsSents = append(rowsSents, float64(r.QueryTime.RowsSent))
		}
		if r.QueryTime.RowsExamined > 0 {
			rowsExamines = append(rowsExamines, float64(r.QueryTime.RowsExamined))
		}
	}

	s.stats = &slowQueryStats{
		ExecTime:    computeStat("Exec Time", queryTimes, s.TotalTime),
		LockTime:    computeStat("Lock Time", lockTimes, s.TotalLockTime),
		RowsSent:    computeStat("Rows Sent", rowsSents, float64(s.TotalRowsSent)),
		RowsExamine: computeStat("Rows Examine", rowsExamines, float64(s.TotalRowsExamined)),
	}
}

func (s *SlowQuerySummary) ComputeHistogram() {
	src := make([]float64, 0, len(s.RawInfo))
	for _, r := range s.RawInfo {
		qus := r.QueryTime.QueryTime * 1000 * 1000
		if qus > 1 {
			src = append(src, qus)
		}
	}

	sort.Float64Slice(src).Sort()

	hist := stat.Histogram(nil, divider, src, nil)

	s.queryTimeHistogram = Histogram(hist)
}

func (s *SlowQuerySummary) appendQueryTime(info *SlowQueryInfo) {
	s.TotalLockTime += info.QueryTime.LockTime
	s.TotalTime += info.QueryTime.QueryTime
	s.TotalRowsSent += info.QueryTime.RowsSent
	s.TotalRowsExamined += info.QueryTime.RowsExamined
	s.RawInfo = append(s.RawInfo, info)

	s.TotalQueryCount++
}

func computeStat(label string, x []float64, total float64) slowQueryStat {
	if len(x) == 0 {
		return slowQueryStat{label: label, total: total}
	}

	sort.Float64Slice(x).Sort()

	min := x[0]
	max := x[len(x)-1]
	median := stat.Quantile(0.5, stat.LinInterp, x, nil)
	avg := total / float64(len(x)-1)
	stddev := stat.StdDev(x, nil)
	quantile := stat.Quantile(0.95, stat.LinInterp, x, nil)

	return slowQueryStat{
		label:    label,
		total:    total,
		min:      min,
		max:      max,
		avg:      avg,
		stddev:   stddev,
		quantile: quantile,
		median:   median,
	}
}

type slowQueryStats struct {
	ExecTime    slowQueryStat
	LockTime    slowQueryStat
	RowsSent    slowQueryStat
	RowsExamine slowQueryStat
}

func (s *slowQueryStats) String() string {
	var b strings.Builder
	t := table.NewWriter()

	t.SetOutputMirror(&b)
	t.AppendHeader(table.Row{"Attribute", "total", "min", "max", "avg", "95%", "stddev", "median"})
	t.AppendRows([]table.Row{
		{s.ExecTime.label, s.ExecTime.total, s.ExecTime.min, s.ExecTime.max, s.ExecTime.quantile, s.ExecTime.stddev, s.ExecTime.median},
		{s.LockTime.label, s.LockTime.total, s.LockTime.min, s.LockTime.max, s.LockTime.quantile, s.LockTime.stddev, s.LockTime.median},
		{s.RowsSent.label, s.RowsSent.total, s.RowsSent.min, s.RowsSent.max, s.RowsSent.quantile, s.RowsSent.stddev, s.RowsSent.median},
		{s.RowsExamine.label, s.RowsExamine.total, s.RowsExamine.min, s.RowsExamine.max, s.RowsExamine.quantile, s.RowsExamine.stddev, s.RowsExamine.median},
	})
	t.Render()

	return b.String()
}

type slowQueryStat struct {
	label    string
	total    float64
	min      float64
	max      float64
	avg      float64
	quantile float64
	stddev   float64
	median   float64
}
