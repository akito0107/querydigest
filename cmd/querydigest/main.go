package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
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

	results, err := analyzeSlowQuery(f, *concurrency)
	if err != nil {
		log.Fatal(err)
	}

	if *previewSize != 0 && *previewSize <= len(results) {
		results = results[0:*previewSize]
	}

	print(os.Stdout, results)
}

func print(w io.Writer, summaries []*querydigest.SlowQuerySummary) {
	for i, s := range summaries {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Query %d\n", i)
		fmt.Fprintf(w, "%s\n", s.String())
		fmt.Println()
	}
}

func analyzeSlowQuery(r io.Reader, concurrency int) ([]*querydigest.SlowQuerySummary, error) {
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

	var qs []*querydigest.SlowQuerySummary

	for _, v := range summarizer.Map() {
		qs = append(qs, v)
	}

	sort.Slice(qs, func(i, j int) bool {
		return qs[i].TotalTime > qs[j].TotalTime
	})

	return qs, nil
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
