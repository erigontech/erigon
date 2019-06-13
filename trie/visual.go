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
func Visual(t *Trie, highlight []byte, w io.Writer) {
	fmt.Fprintf(w,
		`digraph trie {
	node [shape=none margin=0 width=0 height=0]
    edge [dir = none headport=n tailport=s]
`)
	visualNode(t.root, []byte{}, highlight, w)
	fmt.Fprintf(w,
		`}
`)
}

func visualNode(nd node, hex []byte, highlight []byte, w io.Writer) {
	switch n := nd.(type) {
	case nil:
	case *shortNode:
		nKey := compactToHex(n.Key)
		fmt.Fprintf(w,
			`
	n_%x [label=<
	<table border="1" cellborder="0" cellspacing="0">
`, hex)
		for _, h := range nKey {
			if h == 16 {
				continue
			}
			fmt.Fprintf(w,
				`		<tr><td bgcolor="%s"></td></tr>
`, indexColors[h])
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
			var newHighlight []byte
			if highlight != nil && bytes.HasPrefix(highlight, nKey) {
				newHighlight = highlight[len(nKey):]
			}
			visualNode(n.Val, concat(hex, nKey...), newHighlight, w)
		}
	case *duoNode:
		i1, i2 := n.childrenIdx()
		fmt.Fprintf(w,
			`
	n_%x [label=<
	<table border="1" cellborder="0" cellspacing="0">
		<tr>
			<td bgcolor="%s" port="h%d"></td>
			<td bgcolor="%s" port="h%d"></td>
		</tr>
	</table>
    >];
    n_%x:h%d -> n_%x;
    n_%x:h%d -> n_%x;
`, hex, indexColors[i1], i1, indexColors[i2], i2, hex, i1, concat(hex, i1), hex, i2, concat(hex, i2))
		var highlight1 []byte
		if highlight != nil && highlight[0] == i1 {
			highlight1 = highlight[1:]
		}
		var highlight2 []byte
		if highlight != nil && highlight[0] == i2 {
			highlight2 = highlight[1:]
		}
		visualNode(n.child1, concat(hex, i1), highlight1, w)
		visualNode(n.child2, concat(hex, i2), highlight2, w)
	case *fullNode:
		fmt.Fprintf(w,
			`
	n_%x [label=<
	<table border="1" cellborder="0" cellspacing="0">
		<tr>
`, hex)
		for i, child := range n.Children {
			if child == nil {
				continue
			}
			fmt.Fprintf(w,
				`
			<td bgcolor="%s" port="h%d"></td>
`, indexColors[i], i)
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
			var newHighlight []byte
			if highlight != nil && highlight[0] == byte(i) {
				newHighlight = highlight[1:]
			}
			visualNode(child, concat(hex, byte(i)), newHighlight, w)
		}
	}
}
