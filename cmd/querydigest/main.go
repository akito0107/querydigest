package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/akito0107/querydigest"
)

var slowLogPath = flag.String("f", "slow.log", "slow log filepath")
var previewSize = flag.Int("n", 0, "count")
var concurrency = flag.Int("j", 0, "concurrency (default = num of cpus)")

func main() {
	// defer profile.Start(profile.ProfilePath("."), profile.TraceProfile).Stop()
	// defer profile.Start(profile.ProfilePath("."), profile.CPUProfile).Stop()
	flag.Parse()

	f, err := os.Open(*slowLogPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if *concurrency == 0 {
		*concurrency = runtime.NumCPU()
	}

	results, total, err := analyzeSlowQuery(f, *concurrency)
	if err != nil {
		log.Fatal(err)
	}

	if *previewSize != 0 && *previewSize <= len(results) {
		results = results[0:*previewSize]
	}

	print(os.Stdout, results, total)
}

func print(w io.Writer, summaries []*querydigest.SlowQuerySummary, totalTime float64) {
	for i, s := range summaries {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Query %d\n", i)
		fmt.Fprintf(w, "%f%%\n\n", (s.TotalTime/totalTime)*100)
		fmt.Fprintf(w, "%s", s.String())
		fmt.Fprintln(w)
	}
}

func analyzeSlowQuery(r io.Reader, concurrency int) ([]*querydigest.SlowQuerySummary, float64, error) {
	parsequeue := make(chan *querydigest.SlowQueryInfo, 500)

	go parseRawFile(r, parsequeue)

	summarizer := querydigest.NewSummarizer()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range parsequeue {
				res, err := querydigest.ReplaceWithZeroValue(s.RawQuery)
				if err != nil {
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

func parseRawFile(r io.Reader, parsequeue chan *querydigest.SlowQueryInfo) {
	slowqueryscanner := querydigest.NewSlowQueryScanner(r)

	for slowqueryscanner.Next() {
		parsequeue <- slowqueryscanner.SlowQueryInfo()
	}
	if err := slowqueryscanner.Err(); err != nil {
		log.Fatal(err)
	}

	close(parsequeue)
}
