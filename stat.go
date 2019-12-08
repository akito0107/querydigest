package querydigest

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"gonum.org/v1/gonum/stat"
)

type Histogram []float64

var divider = []float64{1, 10, 100, 1000, 10000, 100000, 1000000, math.Inf(0)}
var dividerLabel = []string{
	"1us",
	"10us",
	"100us",
	"1ms",
	"10ms",
	"100ms",
	"1s",
	"10s~",
}

const maxLength = 75

func (h Histogram) String() string {
	tmp := make([]float64, len(h))
	copy(tmp, h)

	sort.Float64Slice(tmp).Sort()
	max := tmp[len(h)-1]

	fmt.Println(tmp)
	unit := maxLength / max

	str := "\n"

	for i := 0; i < len(divider); i++ {
		if len(h) <= i {
			str += fmt.Sprintf("%s:\t\n", dividerLabel[i])
			continue
		}
		length := int(unit * h[i])
		if length <= 0 {
			length = 0
		}
		str += fmt.Sprintf("%s:\t%s\n", dividerLabel[i], strings.Repeat("#", length))
	}

	return str
}

func QueryTimeHistogram(summary *SlowQuerySummary) Histogram {
	src := make([]float64, 0, len(summary.RawInfo))
	for _, r := range summary.RawInfo {
		if r.QueryTime.QueryTime > 0 {
			src = append(src, r.QueryTime.QueryTime*1000*1000)
		}
	}

	sort.Float64Slice(src).Sort()

	hist := stat.Histogram(nil, divider, src, nil)

	return Histogram(hist)
	// fmt.Printf("Hist = %v\n", hist)
}
