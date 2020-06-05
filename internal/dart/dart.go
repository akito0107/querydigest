package dart

import (
	"fmt"
)

type node struct {
	base  int
	check int
}

type PrefixMatcher struct {
	nodes []node
}

const sentinelCode = 26

type trie struct {
	next [sentinelCode + 1]*trie
}

func Build(keys []string) (*PrefixMatcher, error) {
	root, err := buildTrie(keys)
	if err != nil {
		return nil, err
	}

	nodes := make([]node, 256)
	nodes[0] = node{base: 1, check: 0}

	var dfs func(*trie, int)
	dfs = func(t *trie, id int) {
		for c, n := range t.next {
			if n == nil {
				continue
			}
			nid := nodes[id].base+c
			nodes[nid].check = id
			nodes[nid].base = 1
		}
		for c, n := range t.next {
			if n == nil {
				continue
			}
			i := nodes[id].base + c
			for base := 1; ; base++ {
				ok := true
				for nc, nn := range n.next {
					if nn == nil {
						continue
					}
					if nodes[base+nc].base != 0 {
						ok = false
						break
					}
				}
				if ok {
					nodes[i].base = base
					dfs(n, i)
					break
				}
			}
		}
	}
	dfs(root, 0)
	return &PrefixMatcher{nodes: nodes}, nil
}

func Must(pm *PrefixMatcher, err error) *PrefixMatcher {
	if err != nil {
		panic(err)
	}
	return pm
}

func (m *PrefixMatcher) Match(b []byte) bool {
	n := 0
	for _, x := range b {
		base := m.nodes[n].base
		if m.nodes[base+sentinelCode].check == n {
			return true
		}
		c := toCode(x)
		if c < 0 {
			return false
		}
		if m.nodes[base+c].check != n {
			return false
		}
		n = base + c
	}
	base := m.nodes[n].base
	return m.nodes[base+sentinelCode].check == n
}

func toCode(b byte) int {
	if 'A' <= b && b <= 'Z' {
		return int(b - 'A')
	}
	if 'a' <= b && b <= 'z' {
		return int(b - 'a')
	}
	return -1
}

func buildTrie(keys []string) (*trie, error) {
	root := &trie{}

	for _, key := range keys {
		t := root
		for _, b := range []byte(key) {
			c := toCode(b)
			if c < 0 {
				return nil, fmt.Errorf("key(%q) contains unknown", key)
			}
			if t.next[c] == nil {
				t.next[c] = &trie{}
			}
			t = t.next[c]
		}
		if t.next[sentinelCode] == nil {
			t.next[sentinelCode] = &trie{}
		}
	}
	return root, nil
}
