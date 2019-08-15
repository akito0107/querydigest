package main

import (
	"os"
	"testing"
)

func BenchmarkSlowQueryScanner_SlowQueryInfo(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f, err := os.Open("./slow.log")
		if err != nil {
			b.Fatal(err)
		}
		sc := NewSlowQueryScanner(f)

		for sc.Next() {
		}

		if err := sc.Err(); err != nil {
			b.Fatal(err)
		}

		f.Close()
	}
}
