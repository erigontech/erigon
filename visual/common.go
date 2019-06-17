package visual

import (
	"fmt"
	"io"
)

func StartGraph(w io.Writer) {
	fmt.Fprintf(w,
		`digraph trie {
	node [shape=none margin=0 width=0 height=0]
    edge [dir = none headport=n tailport=s]
`)
}

func StartCluster(w io.Writer, name string) {
	fmt.Fprintf(w,
		`subgraph c_%s {
`, name)
}

func EndCluster(w io.Writer) {
	EndGraph(w)
}

func EndGraph(w io.Writer) {
	fmt.Fprintf(w,
		`}
`)
}
