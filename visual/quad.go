package visual

import (
	"fmt"
	"io"
)

// Primitives for drawing hexary strings in graphviz dot format

var quadIndexColors = []string{
	"#FFFFFF", // white
	"#FBF305", // yellow
	"#F20884", // magenta
	"#02ABEA", // cyan
}

var quadFontColors = []string{
	"#000000",
	"#000000",
	"#000000",
	"#000000",
}

var quadIndices = []string{"0", "1", "2", "3"}

// QuadVertical produces a vertical line corresponding to hex digits in key (one byte - one digit)
// highlighted - number of digits that need to be highlighted (contain digits themselves)
// name - name of the compontent (to be connected to others)
func QuadVertical(w io.Writer, quad []byte, highlighted int, name string) {
	fmt.Fprintf(w,
		`
	%s [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0">
	`, name)
	for i, h := range quad {
		if i < highlighted {
			fmt.Fprintf(w,
				`		<tr><td bgcolor="%s"><font color="%s">%s</font></td></tr>
	`, quadIndexColors[h], quadFontColors[h], quadIndices[h])
		} else {
			fmt.Fprintf(w,
				`		<tr><td bgcolor="%s"></td></tr>
	`, quadIndexColors[h])
		}
	}
	fmt.Fprintf(w,
		`
	</table>
	>];
	`)
}
