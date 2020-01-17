package querydigest

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSlowQueryScanner_Next(t *testing.T) {

	cases := []struct {
		name         string
		fixturesPath string
		expect       SlowQueryInfo
	} {
		{
			name: "header",
			fixturesPath: "ignore",
			expect:SlowQueryInfo{
				RawQuery:    "select @@version_comment limit 1;",
				QueryTime:   &QueryTime{
					QueryTime:    0.000126,
					LockTime:     0,
					RowsSent:     1,
					RowsExamined: 0,
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, err := os.Open("./testdata/mysql-slow." + c.fixturesPath + ".log")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			scanner := NewSlowQueryScanner(f)

			scanner.Next()

			if scanner.Err() != nil {
				t.Fatal(scanner.Err())
			}

			info := scanner.SlowQueryInfo()

			if info == nil {
				t.Fatal("info is nil")
			}

			if diff := cmp.Diff(*info, c.expect); diff != "" {
				t.Errorf("diff: %s", diff)
			}

		})
	}

}

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
