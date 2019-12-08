package querydigest

import (
	"sync"
)

type Summarizer struct {
	m         map[string]*SlowQuerySummary
	mu        sync.Mutex
	totalTime float64
}

func NewSummarizer() *Summarizer {
	return &Summarizer{
		m: make(map[string]*SlowQuerySummary),
	}
}

func (s *Summarizer) Map() map[string]*SlowQuerySummary {
	return s.m
}

func (s *Summarizer) TotalQueryTime() float64 {
	return s.totalTime
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
	s.totalTime += i.QueryTime.QueryTime
	s.mu.Unlock()
}
