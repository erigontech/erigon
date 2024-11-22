// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package visual

import (
	"fmt"
	"io"
)

func StartGraph(w io.Writer, tall bool) {
	if tall {
		fmt.Fprintf(w,
			`digraph trie {
		rankdir=LR;
		node [shape=none margin=0 width=0 height=0]
		edge [dir = none headport=w tailport=e]
	`)
	} else {
		fmt.Fprintf(w,
			`digraph trie {
		node [shape=none margin=0 width=0 height=0]
		edge [dir = none headport=n tailport=s]
	`)
	}
}

func EndGraph(w io.Writer) {
	fmt.Fprintf(w,
		`}
`)
}

func Circle(w io.Writer, name string, label string, filled bool) {
	if filled {
		fmt.Fprintf(w,
			`%s [label="%s" margin=0.05 shape=Mrecord fillcolor="#E0E0E0" style=filled];
	`, name, label)
	} else {
		fmt.Fprintf(w,
			`%s [label="%s" margin=0.05 shape=Mrecord];
	`, name, label)
	}
}

func Box(w io.Writer, name string, label string) {
	fmt.Fprintf(w,
		`%s [label="%s" shape=box margin=0.1 width=0 height=0 fillcolor="#FF6403" style=filled];
`, name, label)
}

func StartCluster(w io.Writer, number int, label string) {
	fmt.Fprintf(w,
		`subgraph cluster_%d {
			label = "%s";
			color = black;
`, number, label)
}

func EndCluster(w io.Writer) {
	fmt.Fprintf(w,
		`}
`)
}
