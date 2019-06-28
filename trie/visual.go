// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Visualisation of Merkle Patricia Tries.
package trie

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/visual"
)

// Visual creates visualisation of trie with highlighting
func Visual(t *Trie, highlights [][]byte, w io.Writer, indexColors []string, fontColors []string, values bool) {
	var highlightsHex [][]byte
	for _, h := range highlights {
		highlightsHex = append(highlightsHex, keybytesToHex(h))
	}
	var leaves map[string]struct{}
	if values {
		leaves = make(map[string]struct{})
	}
	hashes := make(map[string]struct{})
	visualNode(t.root, []byte{}, highlightsHex, w, indexColors, fontColors, leaves, hashes)
	fmt.Fprintf(w, "{rank = same;")
	for leaf := range leaves {
		fmt.Fprintf(w, "n_%x;", leaf)
	}
	fmt.Fprintf(w, `};
`)
	fmt.Fprintf(w, "{rank = same;")
	for hash := range hashes {
		fmt.Fprintf(w, "n_%x;", hash)
	}
	fmt.Fprintf(w, `};
`)
}

func visualNode(nd node, hex []byte, highlights [][]byte, w io.Writer, indexColors []string, fontColors []string,
	leaves map[string]struct{}, hashes map[string]struct{}) {
	switch n := nd.(type) {
	case nil:
	case *shortNode:
		nKey := compactToHex(n.Key)
		var pLenMax int
		for _, h := range highlights {
			pLen := prefixLen(nKey, h)
			if pLen > pLenMax {
				pLenMax = pLen
			}
		}
		visual.Vertical(w, nKey, pLenMax, fmt.Sprintf("n_%x", hex), indexColors, fontColors)
		if v, ok := n.Val.(valueNode); !ok {
			fmt.Fprintf(w,
				`

	n_%x -> n_%x;
`, hex, concat(hex, nKey...))
			var newHighlights [][]byte
			for _, h := range highlights {
				if h != nil && bytes.HasPrefix(h, nKey) {
					newHighlights = append(newHighlights, h[len(nKey):])
				}
			}
			visualNode(n.Val, concat(hex, nKey...), newHighlights, w, indexColors, fontColors, leaves, hashes)
		} else {
			if leaves != nil {
				leaves[string(hex)] = struct{}{}
				visual.Circle(w, fmt.Sprintf("e_%s", string(v)), string(v))
				fmt.Fprintf(w,
					`n_%x -> e_%s;
	`, hex, string(v))
			}
		}
	case *duoNode:
		i1, i2 := n.childrenIdx()
		fmt.Fprintf(w,
			`
	n_%x [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0">
		<tr>
`, hex)
		var hOn1, hOn2 bool
		var highlights1, highlights2 [][]byte
		for _, h := range highlights {
			if len(h) > 0 && h[0] == i1 {
				highlights1 = append(highlights1, h[1:])
				hOn1 = true
			}
			if len(h) > 0 && h[0] == i2 {
				highlights2 = append(highlights2, h[1:])
				hOn2 = true
			}
		}
		if hOn1 {
			fmt.Fprintf(w,
				` 
			<td bgcolor="%s" port="h%d"><font color="%s">%s</font></td>
`, indexColors[i1], i1, fontColors[i1], indices[i1])
		} else {
			fmt.Fprintf(w,
				` 
			<td bgcolor="%s" port="h%d"></td>
`, indexColors[i1], i1)
		}
		if hOn2 {
			fmt.Fprintf(w,
				` 
			<td bgcolor="%s" port="h%d"><font color="%s">%s</font></td>
`, indexColors[i2], i2, fontColors[i2], indices[i2])
		} else {
			fmt.Fprintf(w,
				` 
			<td bgcolor="%s" port="h%d"></td>
`, indexColors[i2], i2)
		}
		fmt.Fprintf(w,
			`
		</tr>
	</table>
    >];
    n_%x:h%d -> n_%x;
    n_%x:h%d -> n_%x;
`, hex, i1, concat(hex, i1), hex, i2, concat(hex, i2))
		visualNode(n.child1, concat(hex, i1), highlights1, w, indexColors, fontColors, leaves, hashes)
		visualNode(n.child2, concat(hex, i2), highlights2, w, indexColors, fontColors, leaves, hashes)
	case *fullNode:
		fmt.Fprintf(w,
			`
	n_%x [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0">
		<tr>
`, hex)
		hOn := make(map[byte]struct{})
		for _, h := range highlights {
			if len(h) > 0 {
				hOn[h[0]] = struct{}{}
			}
		}
		for i, child := range n.Children {
			if child == nil {
				continue
			}
			if _, ok := hOn[byte(i)]; ok {
				fmt.Fprintf(w,
					`
			<td bgcolor="%s" port="h%d"><font color="%s">%s</font></td>
`, indexColors[i], i, fontColors[i], indices[i])
			} else {
				fmt.Fprintf(w,
					`
			<td bgcolor="%s" port="h%d"></td>
`, indexColors[i], i)
			}
		}
		fmt.Fprintf(w,
			`
		</tr>
	</table>
    >];
`)
		for i, child := range n.Children {
			if child == nil {
				continue
			}
			fmt.Fprintf(w,
				`	n_%x:h%d -> n_%x;
`, hex, i, concat(hex, byte(i)))
		}
		for i, child := range n.Children {
			if child == nil {
				continue
			}
			var newHighlights [][]byte
			for _, h := range highlights {
				if len(h) > 0 && h[0] == byte(i) {
					newHighlights = append(newHighlights, h[1:])
				}
			}
			visualNode(child, concat(hex, byte(i)), newHighlights, w, indexColors, fontColors, leaves, hashes)
		}
	case hashNode:
		hashes[string(hex)] = struct{}{}
		visual.Box(w, fmt.Sprintf("n_%x", hex), "hash")
	}
}

func (t *Trie) Fold(keys [][]byte) {
	var hexes [][]byte
	for _, key := range keys {
		hexes = append(hexes, keybytesToHex(key))
	}
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	_, t.root = fold(t.root, hexes, h, true)
}

func fold(nd node, hexes [][]byte, h *hasher, isRoot bool) (bool, node) {
	switch n := nd.(type) {
	case *shortNode:
		nKey := compactToHex(n.Key)
		var newHexes [][]byte
		for _, hex := range hexes {
			if bytes.Equal(nKey, hex) {
				var hn common.Hash
				h.hash(n, isRoot, hn[:])
				return true, hashNode(hn[:])
			} else {
				pLen := prefixLen(nKey, hex)
				if pLen > 0 {
					newHexes = append(newHexes, hex[pLen:])
				}
			}
		}
		if len(newHexes) > 0 {
			folded, nn := fold(n.Val, newHexes, h, false)
			n.Val = nn
			if folded {
				var hn common.Hash
				h.hash(n, isRoot, hn[:])
				return true, hashNode(hn[:])
			}
			return false, n
		}
	case *duoNode:
		i1, i2 := n.childrenIdx()
		var hexes1, hexes2 [][]byte
		for _, h := range hexes {
			if len(h) > 0 && h[0] == i1 {
				hexes1 = append(hexes1, h[1:])
			}
			if len(h) > 0 && h[0] == i2 {
				hexes2 = append(hexes2, h[1:])
			}
		}
		var folded1, folded2 bool
		var nn1, nn2 node
		if len(hexes1) > 0 {
			folded1, nn1 = fold(n.child1, hexes1, h, false)
			n.child1 = nn1
		}
		if len(hexes2) > 0 {
			folded2, nn2 = fold(n.child2, hexes2, h, false)
			n.child2 = nn2
		}
		if folded1 && folded2 {
			var hn common.Hash
			h.hash(n, isRoot, hn[:])
			return true, hashNode(hn[:])
		}
		return false, n
	case *fullNode:
		var unfolded bool
		for i, child := range n.Children {
			if child == nil {
				continue
			}
			var newHexes [][]byte
			for _, h := range hexes {
				if len(h) > 0 && h[0] == byte(i) {
					newHexes = append(newHexes, h[1:])
				}
			}
			if len(newHexes) > 0 {
				folded, nn := fold(child, newHexes, h, false)
				n.Children[i] = nn
				if !folded {
					unfolded = true
				}
			} else {
				unfolded = true
			}
		}
		if !unfolded {
			var hn common.Hash
			h.hash(n, isRoot, hn[:])
			return true, hashNode(hn[:])
		}
		return false, n
	}
	return false, nd
}
