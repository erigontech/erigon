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
