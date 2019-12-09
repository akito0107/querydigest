package querydigest

import (
	"fmt"
	"math"
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

	fmt.Fprintf(&b, "Summary:\n")
	fmt.Fprintf(&b, "total query time:\t%0.2fs\n", s.TotalTime)
	fmt.Fprintf(&b, "total query count:\t%d\n\n", s.TotalQueryCount)

	fmt.Fprintf(&b, "%s\n", s.stats)

	fmt.Fprintf(&b, "Query_time distribution:\n%v\n", s.queryTimeHistogram)

	fmt.Fprintf(&b, "QueryExample:\n%s\n", s.RowSample)

	return b.String()
}

func (s *SlowQuerySummary) ComputeStats() {
	queryTimes := make([]float64, 0, len(s.RawInfo))
	lockTimes := make([]float64, 0, len(s.RawInfo))
	rowsSents := make([]float64, 0, len(s.RawInfo))
	rowsExamines := make([]float64, 0, len(s.RawInfo))

	for _, r := range s.RawInfo {
		queryTimes = append(queryTimes, r.QueryTime.QueryTime)
		lockTimes = append(lockTimes, r.QueryTime.LockTime)
		rowsSents = append(rowsSents, float64(r.QueryTime.RowsSent))
		rowsExamines = append(rowsExamines, float64(r.QueryTime.RowsExamined))
	}

	s.stats = &slowQueryStats{
		ExecTime:    computeStatSeconds("Exec Time", queryTimes, s.TotalTime),
		LockTime:    computeStatSeconds("Lock Time", lockTimes, s.TotalLockTime),
		RowsSent:    computeStatCount("Rows Sent", rowsSents, float64(s.TotalRowsSent)),
		RowsExamine: computeStatCount("Rows Examine", rowsExamines, float64(s.TotalRowsExamined)),
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

func computeStatSeconds(label string, x []float64, total float64) slowQueryStatSeconds {
	if len(x) == 0 {
		return slowQueryStatSeconds{label: label, total: seconds(total)}
	}

	sort.Float64Slice(x).Sort()

	// var min float64
	// for _, f := range x {
	// 	if f > 0 {
	// 		min = f
	// 		break
	// 	}
	// }
	min := x[0]
	max := x[len(x)-1]
	median := stat.Quantile(0.5, stat.Empirical, x, nil)
	avg := total / float64(len(x))
	stddev := stat.StdDev(x, nil)
	quantile := stat.Quantile(0.95, stat.Empirical, x, nil)

	return slowQueryStatSeconds{
		label:    label,
		total:    seconds(total),
		min:      seconds(min),
		max:      seconds(max),
		avg:      seconds(avg),
		stddev:   seconds(stddev),
		quantile: seconds(quantile),
		median:   seconds(median),
	}
}

func computeStatCount(label string, x []float64, total float64) slowQueryStatCount {
	if len(x) == 0 {
		return slowQueryStatCount{label: label, total: count(total)}
	}

	sort.Float64Slice(x).Sort()

	min := x[0]
	max := x[len(x)-1]
	median := stat.Quantile(0.5, stat.LinInterp, x, nil)
	avg := total / float64(len(x)-1)
	stddev := stat.StdDev(x, nil)
	quantile := stat.Quantile(0.95, stat.LinInterp, x, nil)

	return slowQueryStatCount{
		label:    label,
		total:    count(total),
		min:      count(min),
		max:      count(max),
		avg:      count(avg),
		stddev:   count(stddev),
		quantile: count(quantile),
		median:   count(median),
	}
}

type slowQueryStats struct {
	ExecTime    slowQueryStatSeconds
	LockTime    slowQueryStatSeconds
	RowsSent    slowQueryStatCount
	RowsExamine slowQueryStatCount
}

func (s *slowQueryStats) String() string {
	var b strings.Builder
	t := table.NewWriter()

	t.SetOutputMirror(&b)
	t.AppendHeader(table.Row{"Attribute", "total", "min", "max", "avg", "95%", "stddev", "median"})
	t.AppendRows([]table.Row{
		{s.ExecTime.label, s.ExecTime.total, s.ExecTime.min, s.ExecTime.max, s.ExecTime.avg, s.ExecTime.quantile, s.ExecTime.stddev, s.ExecTime.median},
		{s.LockTime.label, s.LockTime.total, s.LockTime.min, s.LockTime.max, s.LockTime.avg, s.LockTime.quantile, s.LockTime.stddev, s.LockTime.median},
		{s.RowsSent.label, s.RowsSent.total, s.RowsSent.min, s.RowsSent.max, s.RowsSent.avg, s.RowsSent.quantile, s.RowsSent.stddev, s.RowsSent.median},
		{s.RowsExamine.label, s.RowsExamine.total, s.RowsExamine.min, s.RowsExamine.max, s.RowsExamine.avg, s.RowsExamine.quantile, s.RowsExamine.stddev, s.RowsExamine.median},
	})
	t.Render()

	return b.String()
}

type seconds float64

func (r seconds) String() string {
	if math.IsNaN(float64(r)) || math.IsInf(float64(r), 0) {
		return "-"
	}

	nano := r * 1000 * 1000
	if nano < 1000 {
		return fmt.Sprintf("%.0fus", nano)
	}
	if nano < 1000000 {
		return fmt.Sprintf("%.0fms", nano/1000)
	}
	return fmt.Sprintf("%.0fs", r)
}

type slowQueryStatSeconds struct {
	label    string
	total    seconds
	min      seconds
	max      seconds
	avg      seconds
	quantile seconds
	stddev   seconds
	median   seconds
}

type count float64

func (c count) String() string {
	if math.IsNaN(float64(c)) || math.IsInf(float64(c), 0) {
		return "-"
	}
	return fmt.Sprintf("%.2f", c)
}

type slowQueryStatCount struct {
	label    string
	total    count
	min      count
	max      count
	avg      count
	quantile count
	stddev   count
	median   count
}
