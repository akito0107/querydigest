#!/bin/bash

set +eu

now=`date +%s`
count=${2:-1}
tag=${3:-$now}

resultPath="benchresult/${1}/${tag}"

mkdir -p "${resultPath}"

case "$1" in
    "cli" )  go test -bench BenchmarkRun \
        -benchmem -o "${resultPath}/querydigest.cli.${tag}.out" \
        -cpuprofile "${resultPath}/cpu.cli.${tag}.pprof" \
        -memprofile "${resultPath}/mem.cli.${tag}.pprof" \
        -count "${count}"\
        -cpu 2,4,6,8,12 \
        | tee "${resultPath}/cli.${tag}.txt";;

    "scanner" )  go test -bench BenchmarkSlowQueryScanner_SlowQueryInfo \
        -benchmem -o "${resultPath}/querydigest.scanner.${tag}.out" \
        -cpuprofile "${resultPath}/cpu.scanner.${tag}.pprof" \
        -memprofile "${resultPath}/mem.scanner.${tag}.pprof" \
        -count "${count}"\
        -cpu 2,4,6,8,12 \
        | tee "${resultPath}/scanner.${tag}.txt";;
    *)
        rm -rf ${resultPath};
        echo "unknown command";
esac
