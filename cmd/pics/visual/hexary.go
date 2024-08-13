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

var HexIndexColors = []string{
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

var HexFontColors = []string{
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

// Vertical produces a vertical line corresponding to hex digits in key (one byte - one digit)
// highlighted - number of digits that need to be highlighted (contain digits themselves)
// name - name of the compontent (to be connected to others)
func Vertical(w io.Writer, hex []byte, highlighted int, name string, indexColors []string, fontColors []string, compression int) {
	fmt.Fprintf(w,
		`
	%s [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0">
	`, name)
	if hex[len(hex)-1] == 16 {
		hex = hex[:len(hex)-1]
	} else {
		compression = 0 // No compression for non-terminal keys
	}
	for i, h := range hex {
		if i < len(hex)-compression-2 || i > len(hex)-3 {
			if i < highlighted {
				fmt.Fprintf(w,
					`		<tr><td bgcolor="%s"><font color="%s">%s</font></td></tr>
		`, indexColors[h], fontColors[h], hexIndices[h])
			} else {
				fmt.Fprintf(w,
					`		<tr><td bgcolor="%s"></td></tr>
		`, indexColors[h])
			}
		} else if compression > 0 && i == len(hex)-3 {
			fmt.Fprintf(w,
				`		<tr><td border="0">|</td></tr>
			`)
		}
	}
	fmt.Fprintf(w,
		`
	</table>
	>];
	`)
}

func Horizontal(w io.Writer, hex []byte, highlighted int, name string, indexColors []string, fontColors []string, compression int) {
	fmt.Fprintf(w,
		`
	%s [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0">
	<tr>`, name)
	if len(hex) > 0 && hex[len(hex)-1] == 16 {
		hex = hex[:len(hex)-1]
	}
	if len(hex) == 0 {
		fmt.Fprintf(w, "<td></td>")
	}
	for i, h := range hex {
		if i < len(hex)-compression-2 || i > len(hex)-3 {
			if i < highlighted {
				fmt.Fprintf(w,
					`		<td bgcolor="%s"><font color="%s">%s</font></td>
		`, indexColors[h], fontColors[h], hexIndices[h])
			} else {
				fmt.Fprintf(w,
					`		<td bgcolor="%s"></td>
		`, indexColors[h])
			}
		} else if compression > 0 && i == len(hex)-3 {
			fmt.Fprintf(w,
				`		<td border="0"><font point-size="1">-----------</font></td>
			`)
		}
	}
	fmt.Fprintf(w,
		`
	</tr></table>
	>];
	`)
}

func HexBox(w io.Writer, name string, code []byte, columns int, compressed bool, highlighted bool) {
	fmt.Fprintf(w,
		`
	%s [label=<
	<table border="0" color="#000000" cellborder="1" cellspacing="0">
	`, name)
	rows := (len(code) + columns - 1) / columns
	row := 0
	for rowStart := 0; rowStart < len(code); rowStart += columns {
		if rows < 6 || !compressed || row < 2 || row > rows-3 {
			fmt.Fprintf(w, "		<tr>")
			col := 0
			for ; rowStart+col < len(code) && col < columns; col++ {
				if columns < 6 || !compressed || col < 2 || col > columns-3 {
					h := code[rowStart+col]
					if highlighted {
						fmt.Fprintf(w,
							`		<td bgcolor="%s"><font color="%s">%s</font></td>
				`, HexIndexColors[h], HexFontColors[h], hexIndices[h])
					} else {
						fmt.Fprintf(w, `<td bgcolor="%s"></td>`, HexIndexColors[h])
					}
				}
				if compressed && columns >= 6 && col == 2 && (row == 0 || row == rows-2) {
					fmt.Fprintf(w, `<td rowspan="2" border="0"></td>`)
				}
			}
			if col < columns {
				fmt.Fprintf(w, `<td colspan="%d" border="0"></td>`, columns-col)
			}
			fmt.Fprintf(w, `</tr>
		`)
		}
		if compressed && rows >= 6 && row == 2 {
			fmt.Fprintf(w, "		<tr>")
			fmt.Fprintf(w, `<td colspan="%d" border="0"></td>`, columns)
			fmt.Fprintf(w, `</tr>
		`)
		}
		row++
	}
	fmt.Fprintf(w,
		`
	</table>
	>];
	`)
}
