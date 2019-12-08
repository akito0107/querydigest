package querydigest

import (
	"fmt"
	"sort"
	"strings"

	"gonum.org/v1/gonum/stat"
)

type SlowQuerySummary struct {
	RowSample         string
	TotalTime         float64
	TotalLockTime     float64
	TotalQueryCount   int
	TotalRowsSent     int
	TotalRowsExamined int
	RawInfo           []*SlowQueryInfo
}

func (s *SlowQuerySummary) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "row: %s\n", s.RowSample)
	fmt.Fprintf(&b, "query time(sec): %f\n", s.TotalTime)
	fmt.Fprintf(&b, "total query count: %d\n", s.TotalQueryCount)
	fmt.Fprintf(&b, "Query_time distribution:\n%v\n", s.ComputeHistogram())

	return b.String()
}

func (s *SlowQuerySummary) appendQueryTime(info *SlowQueryInfo) {
	s.TotalLockTime += info.QueryTime.LockTime
	s.TotalTime += info.QueryTime.QueryTime
	s.TotalRowsSent += info.QueryTime.RowsSent
	s.TotalRowsExamined += info.QueryTime.RowsExamined
	s.RawInfo = append(s.RawInfo, info)

	s.TotalQueryCount += 1
}

func (s *SlowQuerySummary) ComputeHistogram() Histogram {
	src := make([]float64, 0, len(s.RawInfo))
	for _, r := range s.RawInfo {
		if r.QueryTime.QueryTime > 0 {
			src = append(src, r.QueryTime.QueryTime*1000*1000)
		}
	}

	sort.Float64Slice(src).Sort()

	hist := stat.Histogram(nil, divider, src, nil)

	return Histogram(hist)
}
