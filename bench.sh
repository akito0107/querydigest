#!/bin/bash

set +eu

now=`date +%s`
count=${2:-1}
tag=${3:-$now}

resultPath="benchresult/${1}/${tag}"

mkdir -p "${resultPath}"

case "$1" in
    "cli" )  go test -bench BenchmarkRun \
        -benchmem -o "${resultPath}/querydigest.out" \
        -cpuprofile "${resultPath}/cpu.pprof" \
        -memprofile "${resultPath}/mem.pprof" \
        -count "${count}"\
        -cpu 2,4,6,8,12 \
        | tee "${resultPath}/cli.txt";;

    "scanner" )  go test -bench BenchmarkSlowQueryScanner_SlowQueryInfo \
        -benchmem -o "${resultPath}/querydigest.out" \
        -cpuprofile "${resultPath}/cpu.pprof" \
        -memprofile "${resultPath}/mem.pprof" \
        -count "${count}"\
        | tee "${resultPath}/scanner.txt";;
    *)
        rm -rf ${resultPath};
        echo "unknown command";
esac
