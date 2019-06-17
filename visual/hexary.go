package visual

import (
	"fmt"
	"io"
)

// Primitives for drawing hexary strings in graphviz dot format

var hexIndexColors = []string{
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

var hexFontColors = []string{
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

var hexIndices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}

// HexVertical produces a vertical line corresponding to hex digits in key (one byte - one digit)
// highlighted - number of digits that need to be highlighted (contain digits themselves)
// name - name of the compontent (to be connected to others)
func HexVertical(w io.Writer, hex []byte, highlighted int, name string) {
	fmt.Fprintf(w,
		`
	%s [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0">
	`, name)
	for i, h := range hex {
		if h == 16 {
			continue
		}
		if i < highlighted {
			fmt.Fprintf(w,
				`		<tr><td bgcolor="%s"><font color="%s">%s</font></td></tr>
	`, hexIndexColors[h], hexFontColors[h], hexIndices[h])
		} else {
			fmt.Fprintf(w,
				`		<tr><td bgcolor="%s"></td></tr>
	`, hexIndexColors[h])
		}
	}
	fmt.Fprintf(w,
		`
	</table>
	>];
	`)
}
