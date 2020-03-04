package querydigest

import (
	"io/ioutil"
	"os"
	"runtime"
	"testing"
)

func BenchmarkRun(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f, err := os.Open("./benchdata/mysql-slow.log")
		if err != nil {
			b.Fatal(err)
		}

		Run(ioutil.Discard, f, 0, runtime.GOMAXPROCS(0))

		f.Close()
	}
}
