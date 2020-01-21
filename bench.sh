#!/bin/bash

set +eu

now=`date +%s`
count=${2:-1}

case "$1" in
    "cli" )  go test -bench BenchmarkRun -benchmem -o querydigest.out -cpuprofile "cpu.cli.${now}.pprof" -memprofile "mem.cli.${now}.pprof" -count "${count}"\
        | tee "cli.${now}.txt";;
    "scanner" )  go test -bench BenchmarkSlowQueryScanner_SlowQueryInfo \
        -benchmem -o querydigest.out -cpuprofile "cpu.scanner.${now}.pprof" -memprofile "mem.scanner.${now}.pprof" -count "${count}"\
        | tee "scanner.${now}.txt";;
esac
