package querydigest

import (
	"sync"
)

type Summarizer struct {
	m  map[string]*SlowQuerySummary
	mu sync.Mutex
}

func NewSummarizer() *Summarizer {
	return &Summarizer{
		m: make(map[string]*SlowQuerySummary),
	}
}

func (s *Summarizer) Map() map[string]*SlowQuerySummary {
	return s.m
}

func (s *Summarizer) Collect(i *SlowQueryInfo) {
	s.mu.Lock()
	summary, ok := s.m[i.ParsedQuery]
	if !ok {
		summary = &SlowQuerySummary{
			RowSample: i.RawQuery,
		}
	}
	summary.appendQueryTime(i)
	s.m[i.ParsedQuery] = summary
	s.mu.Unlock()
}

type SlowQuerySummary struct {
	RowSample         string
	TotalTime         float64
	TotalLockTime     float64
	TotalQueryCount   int
	TotalRowsSent     int
	TotalRowsExamined int
	RawInfo           []*SlowQueryInfo
}

func (s *SlowQuerySummary) appendQueryTime(info *SlowQueryInfo) {
	s.TotalLockTime += info.QueryTime.LockTime
	s.TotalTime += info.QueryTime.QueryTime
	s.TotalRowsSent += info.QueryTime.RowsSent
	s.TotalRowsExamined += info.QueryTime.RowsExamined
	s.RawInfo = append(s.RawInfo, info)

	s.TotalQueryCount += 1
}
