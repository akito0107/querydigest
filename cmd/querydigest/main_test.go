package main

import (
	"os"
	"testing"

	"github.com/akito0107/querydigest"
)

func BenchmarkSlowQueryScanner_SlowQueryInfo(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f, err := os.Open("../../mysql-slow.log")
		if err != nil {
			b.Fatal(err)
		}
		sc := querydigest.NewSlowQueryScanner(f)

		for sc.Next() {
		}

		if err := sc.Err(); err != nil {
			b.Fatal(err)
		}

		f.Close()
	}
}
