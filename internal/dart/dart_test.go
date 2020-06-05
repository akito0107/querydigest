package dart

import (
	"reflect"
	"testing"
)

func TestPrefixMatcher_Match(t *testing.T) {
	statement := []string{
		"SELECT",
		"INSERT",
		"UPDATE",
		"DELETE",
		"WITH",
		"ALTER",
	}

	tests := []struct {
		name string
		keys []string
		s    string
		want bool
	}{
		{
			keys: []string{
				"A",
				"BC",
			},
			s:    "BCD",
			want: true,
		},
		{
			keys: statement,
			s:    "SELECT * FROM t;",
			want: true,
		},
		{
			keys: statement,
			s:    "SELECT",
			want: true,
		},
		{
			keys: statement,
			s:    "INSERT",
			want: true,
		},
		{
			keys: statement,
			s:    "UPDATE",
			want: true,
		},
		{
			keys: statement,
			s:    "WITH",
			want: true,
		},
		{
			keys: statement,
			s:    "DELETE",
			want: true,
		},
		{
			keys: statement,
			s:    "ALTER",
			want: true,
		},
		{
			keys: statement,
			s:    " SELECT",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Build(tt.keys)
			if err != nil {
				t.Fatal(err)
			}
			if got := m.Match([]byte(tt.s)); got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuild(t *testing.T) {
	type args struct {
		keys []string
	}
	tests := []struct {
		name    string
		args    args
		want    *PrefixMatcher
		wantErr bool
	}{
		{
			args: args{[]string{"A", "BC"}},
			want: &PrefixMatcher{
				nodes: func() []node {
					n := make([]node, 256)
					n[0] = node{base: 1, check: 0}
					n[1+0] = node{base: 1, check: 0}
					n[1+26] = node{base: 1, check: 1}

					n[1+1] = node{base: 1, check: 0}
					n[1+2] = node{base: 2, check: 2}
					n[2+26] = node{base: 1, check: 3}

					return n
				}(),
			},
		},
		{
			args: args{[]string{"SELECT", "INSERT"}},
			want: &PrefixMatcher{
				nodes: func() []node {
					n := make([]node, 256)
					n[0] = node{base: 1, check: 0}

					n[1+8] = node{base: 1, check: 0}  // I
					n[1+18] = node{base: 2, check: 0} // S

					n[1+13] = node{base: 2, check: 9}  // N
					n[2+18] = node{base: 1, check: 14} // S
					n[1+4] = node{base: 1, check: 20}  // E
					n[1+17] = node{base: 2, check: 5}  // R
					n[2+19] = node{base: 1, check: 18} // T
					n[1+26] = node{base: 1, check: 21} // $

					n[2+4] = node{base: 1, check: 19}  // E
					n[1+11] = node{base: 3, check: 6}  // L
					n[3+4] = node{base: 1, check: 12}  // E
					n[1+2] = node{base: 3, check: 7}   // C
					n[3+19] = node{base: 2, check: 3}  // T
					n[2+26] = node{base: 1, check: 22} // $

					return n
				}(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Build(tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Build() got = %+v, want %+v", got.nodes[:32], tt.want.nodes[:32])
			}
		})
	}
}
