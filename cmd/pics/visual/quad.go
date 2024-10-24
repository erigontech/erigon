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

// Primitives for drawing hexary strings in graphviz dot format

var QuadIndexColors = []string{
	"#FFFFFF", // white
	"#FBF305", // yellow
	"#F20884", // magenta
	"#02ABEA", // cyan
}

var QuadFontColors = []string{
	"#000000",
	"#000000",
	"#000000",
	"#000000",
}

var quadIndices = []string{"0", "1", "2", "3"}

// QuadVertical produces a vertical line corresponding to quad digits in key (one byte - one digit)
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
	`, QuadIndexColors[h], QuadFontColors[h], quadIndices[h])
		} else {
			fmt.Fprintf(w,
				`		<tr><td bgcolor="%s"></td></tr>
	`, QuadIndexColors[h])
		}
	}
	fmt.Fprintf(w,
		`
	</table>
	>];
	`)
}

// QuadHorizontal produces a horizontal line corresponding to quad digits in key (one byte - one digit)
// highlighted - whether digits need to be highlighted (contain digits themselves)
// name - name of the compontent (to be connected to others)
func QuadHorizontal(w io.Writer, quad []byte, highlighted bool, name string) {
	fmt.Fprintf(w,
		`
	%s [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0"><tr>
	`, name)
	for _, h := range quad {
		if highlighted {
			fmt.Fprintf(w,
				`		<td bgcolor="%s" port="q%s"><font color="%s">%s</font></td></tr>
	`, QuadIndexColors[h], quadIndices[h], QuadFontColors[h], quadIndices[h])
		} else {
			fmt.Fprintf(w,
				`		<td bgcolor="%s" port="q%s"></td>
	`, QuadIndexColors[h], quadIndices[h])
		}
	}
	fmt.Fprintf(w,
		`
	</tr></table>
	>];
	`)
}
