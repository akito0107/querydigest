#!/bin/bash

set +eu

old=${2}
current=${3}

benchstat "benchresult/${1}/${old}/${1}.${old}.txt" "benchresult/${1}/${current}/${1}.${current}.txt"
