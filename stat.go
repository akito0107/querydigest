package querydigest

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type Histogram []float64

var divider = []float64{1, 10, 100, 1000, 10000, 100000, 1000000, math.Inf(0)}
var dividerLabel = []string{
	"  1us",
	" 10us",
	"100us",
	"  1ms",
	" 10ms",
	"100ms",
	"   1s",
	" 10s~",
}

const maxLength = 75

func (h Histogram) String() string {
	tmp := make([]float64, len(h))
	copy(tmp, h)

	sort.Float64Slice(tmp).Sort()
	max := tmp[len(h)-1]
	unit := maxLength / max

	var b strings.Builder

	for i := 0; i < len(divider); i++ {
		if len(h) <= i {
			fmt.Fprintf(&b, "%s:\t\n", dividerLabel[i])
			continue
		}
		length := int(unit * h[i])
		if length <= 0 {
			length = 0
		}
		fmt.Fprintf(&b, "%s:\t%s\n", dividerLabel[i], strings.Repeat("#", length))
	}

	return b.String()
}
