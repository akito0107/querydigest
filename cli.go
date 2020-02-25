package querydigest

import (
	"fmt"
	"io"
	"log"
	"sync"
)

func Run(w io.Writer, src io.Reader, previewSize, concurrency int) {

	results, total, err := analyzeSlowQuery(src, concurrency)
	if err != nil {
		log.Fatal(err)
	}

	if previewSize != 0 && previewSize <= len(results) {
		results = results[0:previewSize]
	}

	print(w, results, total)
}

func print(w io.Writer, summaries []*SlowQuerySummary, totalTime float64) {
	for i, s := range summaries {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Query %d\n", i)
		fmt.Fprintf(w, "%f%%\n\n", (s.TotalTime/totalTime)*100)
		fmt.Fprintf(w, "%s", s.String())
		fmt.Fprintln(w)
	}
}

func analyzeSlowQuery(r io.Reader, concurrency int) ([]*SlowQuerySummary, float64, error) {
	parsequeue := make(chan *SlowQueryInfo, 500)

	go parseRawFile(r, parsequeue)

	summarizer := NewSummarizer()

	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {

		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range parsequeue {
				res, err := ReplaceWithZeroValue(s.RawQuery)
				if err != nil {
					b := s.RawQuery
					if len(b) > 60 {
						b = b[:60]
					}
					log.Print("replace failed: ", string(b))
					continue
				}
				s.ParsedQuery = res
				summarizer.Collect(s)
			}
		}()
	}
	wg.Wait()

	qs := summarizer.Summarize()

	return qs, summarizer.TotalQueryTime(), nil
}

func parseRawFile(r io.Reader, parsequeue chan *SlowQueryInfo) {
	slowqueryscanner := NewSlowQueryScanner(r)

	for slowqueryscanner.Next() {
		parsequeue <- slowqueryscanner.SlowQueryInfo().clone()
	}
	if err := slowqueryscanner.Err(); err != nil {
		log.Fatal(err)
	}

	close(parsequeue)
}
