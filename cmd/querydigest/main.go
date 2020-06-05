package main

import (
	"flag"
	"log"
	"os"
	"runtime"

	"github.com/akito0107/querydigest"
)

var slowLogPath = flag.String("f", "slow.log", "slow log filepath")
var previewSize = flag.Int("n", 0, "count")
var concurrency = flag.Int("j", 0, "concurrency (default = num of cpus)")

func main() {
	// defer profile.Start(profile.ProfilePath("."), profile.TraceProfile).Stop()
	// defer profile.Start(profile.ProfilePath("."), profile.CPUProfile).Stop()
	// defer profile.Start(profile.ProfilePath("."), profile.MemProfile).Stop()

	flag.Parse()

	f, err := os.Open(*slowLogPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if *concurrency == 0 {
		*concurrency = runtime.NumCPU()
	}

	querydigest.Run(os.Stdout, f, *previewSize, *concurrency)
}
