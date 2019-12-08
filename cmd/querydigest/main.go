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

var slowLogPath = flag.String("f", "slow.log", "slow log filepath (default slow.log)")
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

	if err := parseSlowQuery(f, *concurrency); err != nil {
		log.Fatal(err)
	}
}

func parseSlowQuery(r io.Reader, concurrency int) error {
	parsequeue := make(chan *querydigest.SlowQueryInfo, 500)

	go parseRawFile(r, parsequeue)

	summ := querydigest.NewSummarizer()

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

				summ.Collect(s)
			}
		}()
	}
	wg.Wait()

	var qs []*querydigest.SlowQuerySummary

	for _, v := range summ.Map() {
		qs = append(qs, v)
	}

	sort.Slice(qs, func(i, j int) bool {
		return qs[i].TotalTime > qs[j].TotalTime
	})

	for _, s := range qs {
		fmt.Println("------------------------------")
		fmt.Printf("row: %s\n", s.RowSample)
		fmt.Printf("query time: %f\ns", s.TotalTime)
		fmt.Printf("total query count: %d\n", s.TotalQueryCount)
		fmt.Println("------------------------------")
	}

	return nil
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
