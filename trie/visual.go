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
)

var indexColors = []string{
	"#FFFFFF", // white
	"#FBF305", // yellow
	"#FF6403", // orange
	"#DD0907", // red
	"#F20884", // magenta
	"#4700A5", // purple
	"#0000D3", // blue
	"#02ABEA", // cyan
	"#1FB714", // green
	"#006412", // dark green
	"#562C05", // brown
	"#90713A", // tan
	"#C0C0C0", // light grey
	"#808080", // medium grey
	"#404040", // dark grey
	"#000000", // black
}

var fontColors = []string{
	"#000000",
	"#000000",
	"#000000",
	"#000000",
	"#000000",
	"#FFFFFF",
	"#FFFFFF",
	"#000000",
	"#000000",
	"#FFFFFF",
	"#FFFFFF",
	"#000000",
	"#000000",
	"#000000",
	"#FFFFFF",
	"#FFFFFF",
}

// Visualisation of trie without any highlighting
func Visual(t *Trie, highlights [][]byte, w io.Writer) {
	fmt.Fprintf(w,
		`digraph trie {
	node [shape=none margin=0 width=0 height=0]
    edge [dir = none headport=n tailport=s]
`)
	var highlightsHex [][]byte
	for _, h := range highlights {
		highlightsHex = append(highlightsHex, keybytesToHex(h))
	}
	visualNode(t.root, []byte{}, highlightsHex, w)
	fmt.Fprintf(w,
		`}
`)
}

func visualNode(nd node, hex []byte, highlights [][]byte, w io.Writer) {
	switch n := nd.(type) {
	case nil:
	case *shortNode:
		nKey := compactToHex(n.Key)
		fmt.Fprintf(w,
			`
	n_%x [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0">
`, hex)
		var pLenMax int
		for _, h := range highlights {
			pLen := prefixLen(nKey, h)
			if pLen > pLenMax {
				pLenMax = pLen
			}
		}
		for i, h := range nKey {
			if h == 16 {
				continue
			}
			if i < pLenMax {
				fmt.Fprintf(w,
					`		<tr><td bgcolor="%s"><font color="%s">%s</font></td></tr>
`, indexColors[h], fontColors[h], indices[h])
			} else {
				fmt.Fprintf(w,
					`		<tr><td bgcolor="%s"></td></tr>
`, indexColors[h])
			}
		}
		fmt.Fprintf(w,
			`
	</table>
    >];
`)
		if _, ok := n.Val.(valueNode); !ok {
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
			visualNode(n.Val, concat(hex, nKey...), newHighlights, w)
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
		visualNode(n.child1, concat(hex, i1), highlights1, w)
		visualNode(n.child2, concat(hex, i2), highlights2, w)
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
			visualNode(child, concat(hex, byte(i)), newHighlights, w)
		}
	}
}
