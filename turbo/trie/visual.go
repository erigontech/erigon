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
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/visual"
)

// VisualOpts contains various configuration options fo the Visual function
// It has been introduced as a replacement for too many arguments with options
type VisualOpts struct {
	Highlights     [][]byte // Collection of keys, in the HEX encoding, that need to be highlighted with digits
	IndexColors    []string // Array of colors for representing digits as colored boxes
	FontColors     []string // Array of colors, the same length as indexColors, for the textual digits inside the coloured boxes
	CutTerminals   int      // Specifies how many digits to cut from the terminal short node keys for a more convinient display
	Values         bool     // Whether to display value nodes (as box with rounded corners)
	CodeCompressed bool     // Whether to turn the code from a large rectangle to a small square for a more convinient display
	ValCompressed  bool     // Whether long values (over 10 characters) are shortened using ... in the middle
	ValHex         bool     // Whether values should be displayed as hex numbers (otherwise they are displayed as just strings)
	SameLevel      bool     // Whether the leaves (and hashes) need to be on the same horizontal level
}

// Visual creates visualisation of trie with highlighting
func Visual(t *Trie, w io.Writer, opts *VisualOpts) {
	var leaves map[string]struct{}
	if opts.Values {
		leaves = make(map[string]struct{})
	}
	hashes := make(map[string]struct{})
	visualNode(t.root, []byte{}, w, opts.Highlights, opts, leaves, hashes)
	if opts.SameLevel {
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
}

func visualNode(nd node, hex []byte, w io.Writer, highlights [][]byte, opts *VisualOpts,
	leaves map[string]struct{}, hashes map[string]struct{}) {
	switch n := nd.(type) {
	case nil:
	case *shortNode:
		var pLenMax int
		for _, h := range highlights {
			pLen := prefixLen(n.Key, h)
			if pLen > pLenMax {
				pLenMax = pLen
			}
		}
		visual.Vertical(w, n.Key, pLenMax, fmt.Sprintf("n_%x", hex), opts.IndexColors, opts.FontColors, opts.CutTerminals)
		if v, ok := n.Val.(valueNode); ok {
			if leaves != nil {
				leaves[string(hex)] = struct{}{}
				/*
					var valStr string
					if opts.ValHex {
						valStr = fmt.Sprintf("%x", []byte(v))
					} else {
						valStr = string(v)
					}
					if opts.ValCompressed && len(valStr) > 10 {
						valStr = fmt.Sprintf("%x..%x", []byte(v)[:2], []byte(v)[len(v)-2:])
					}
				*/
				valHex := keybytesToHex(v)
				valHex = valHex[:len(valHex)-1]
				visual.HexBox(w, fmt.Sprintf("e_%x", concat(hex, n.Key...)), valHex, 32, opts.ValCompressed, false)
				fmt.Fprintf(w,
					`n_%x -> e_%x;
	`, hex, concat(hex, n.Key...))
			}
		} else if a, ok := n.Val.(*accountNode); ok {
			balance := float64(big.NewInt(0).Div(a.Balance.ToBig(), big.NewInt(1000000000000000)).Uint64()) / 1000.0
			visual.Circle(w, fmt.Sprintf("e_%x", concat(hex, n.Key...)), fmt.Sprintf("%d \u039E%.3f", a.Nonce, balance), true)
			accountHex := concat(hex, n.Key...)
			fmt.Fprintf(w,
				`n_%x -> e_%x;
`, hex, accountHex)
			if !a.IsEmptyCodeHash() {
				if code := a.code; code != nil {
					codeHex := keybytesToHex(code)
					codeHex = codeHex[:len(codeHex)-1]
					visual.HexBox(w, fmt.Sprintf("c_%x", accountHex), codeHex, 32, opts.CodeCompressed, false)
				} else {
					visual.Box(w, fmt.Sprintf("c_%x", accountHex), "codeHash")
				}
				fmt.Fprintf(w,
					`e_%x -> c_%x;
				`, accountHex, accountHex)
			}
			if !a.IsEmptyRoot() {
				if a.storage != nil {
					nKey := n.Key
					if nKey[len(nKey)-1] == 16 {
						nKey = nKey[:len(nKey)-1]
					}
					var newHighlights [][]byte
					for _, h := range highlights {
						if h != nil && bytes.HasPrefix(h, nKey) {
							newHighlights = append(newHighlights, h[len(nKey):])
						}
					}
					visualNode(a.storage, accountHex[:len(accountHex)-1], w, newHighlights, opts, leaves, hashes)
				} else {
					visual.Box(w, fmt.Sprintf("n_%x", accountHex[:len(accountHex)-1]), "storHash")
				}
				fmt.Fprintf(w,
					`e_%x -> n_%x;
	`, accountHex, accountHex[:len(accountHex)-1])
			}
		} else {
			fmt.Fprintf(w,
				`

	n_%x -> n_%x;
`, hex, concat(hex, n.Key...))
			var newHighlights [][]byte
			for _, h := range highlights {
				if h != nil && bytes.HasPrefix(h, n.Key) {
					newHighlights = append(newHighlights, h[len(n.Key):])
				}
			}
			visualNode(n.Val, concat(hex, n.Key...), w, newHighlights, opts, leaves, hashes)
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
`, opts.IndexColors[i1], i1, opts.FontColors[i1], indices[i1])
		} else {
			fmt.Fprintf(w,
				` 
			<td bgcolor="%s" port="h%d"></td>
`, opts.IndexColors[i1], i1)
		}
		if hOn2 {
			fmt.Fprintf(w,
				` 
			<td bgcolor="%s" port="h%d"><font color="%s">%s</font></td>
`, opts.IndexColors[i2], i2, opts.FontColors[i2], indices[i2])
		} else {
			fmt.Fprintf(w,
				` 
			<td bgcolor="%s" port="h%d"></td>
`, opts.IndexColors[i2], i2)
		}
		fmt.Fprintf(w,
			`
		</tr>
	</table>
    >];
    n_%x:h%d -> n_%x;
    n_%x:h%d -> n_%x;
`, hex, i1, concat(hex, i1), hex, i2, concat(hex, i2))
		visualNode(n.child1, concat(hex, i1), w, highlights1, opts, leaves, hashes)
		visualNode(n.child2, concat(hex, i2), w, highlights2, opts, leaves, hashes)
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
`, opts.IndexColors[i], i, opts.FontColors[i], indices[i])
			} else {
				fmt.Fprintf(w,
					`
			<td bgcolor="%s" port="h%d"></td>
`, opts.IndexColors[i], i)
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
			visualNode(child, concat(hex, byte(i)), w, newHighlights, opts, leaves, hashes)
		}
	case hashNode:
		hashes[string(hex)] = struct{}{}
		visual.Box(w, fmt.Sprintf("n_%x", hex), "hash")
	}
}

// Fold modifies the trie by folding the given set of keys, making sure that they are inaccessible
// without resolution via DB
func (t *Trie) Fold(keys [][]byte) {
	var hexes = make([][]byte, 0, len(keys))
	for _, key := range keys {
		hexes = append(hexes, keybytesToHex(key))
	}
	h := newHasher(false)
	defer returnHasherToPool(h)
	_, t.root = fold(t.root, hexes, h, true)
}

func fold(nd node, hexes [][]byte, h *hasher, isRoot bool) (bool, node) {
	switch n := nd.(type) {
	case *shortNode:
		var newHexes [][]byte
		for _, hex := range hexes {
			if bytes.Equal(n.Key, hex) {
				var hn libcommon.Hash
				h.hash(n, isRoot, hn[:])
				return true, hashNode{hash: hn[:]}
			}
			pLen := prefixLen(n.Key, hex)
			if pLen > 0 {
				newHexes = append(newHexes, hex[pLen:])
			}
		}
		if len(newHexes) > 0 {
			folded, nn := fold(n.Val, newHexes, h, false)
			n.Val = nn
			if folded {
				var hn libcommon.Hash
				h.hash(n, isRoot, hn[:])
				return true, hashNode{hash: hn[:]}
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
			var hn libcommon.Hash
			h.hash(n, isRoot, hn[:])
			return true, hashNode{hash: hn[:]}
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
			var hn libcommon.Hash
			h.hash(n, isRoot, hn[:])
			return true, hashNode{hash: hn[:]}
		}
		return false, n
	}
	return false, nd
}

// HexToQuad converts hexary trie to quad trie with the same set of keys
func HexToQuad(t *Trie) *Trie {
	newTrie := New(libcommon.Hash{})
	transformSubTrie(t.root, []byte{}, newTrie, keyHexToQuad)
	return newTrie
}

// KeyToQuad converts a key in KEY encoding to QUAD encoding (similar to HEX encoding, but uses digits 0..3 instead of digits 0..15)
func KeyToQuad(key []byte) []byte {
	l := len(key)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range key {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return keyHexToQuad(nibbles)
}

func keyHexToQuad(hex []byte) []byte {
	quadLen := len(hex) * 2
	if hex[len(hex)-1] == 16 {
		quadLen--
	}
	quad := make([]byte, quadLen)
	qi := 0
	for _, h := range hex {
		if h == 16 {
			quad[qi] = 16
			qi++
		} else {
			quad[qi] = h / 4
			qi++
			quad[qi] = h % 4
			qi++
		}
	}
	return quad
}

// FullKeys construct the list of full keys (i.e. keys that can be accessed without resolution via DB) that are present in
// the given trie
func FullKeys(t *Trie) []string {
	return fullKeys(t.root, nil, nil)
}

func fullKeys(nd node, hex []byte, fk []string) []string {
	switch n := nd.(type) {
	case nil:
		return fk
	case hashNode:
		return fk
	case valueNode:
		return append(fk, string(concat(hex, 16)))
	case *shortNode:
		h := n.Key
		// Remove terminator
		if h[len(h)-1] == 16 {
			h = h[:len(h)-1]
		}
		hexVal := concat(hex, h...)
		return fullKeys(n.Val, hexVal, fk)
	case *duoNode:
		i1, i2 := n.childrenIdx()
		hex1 := make([]byte, len(hex)+1)
		copy(hex1, hex)
		hex1[len(hex)] = i1
		hex2 := make([]byte, len(hex)+1)
		copy(hex2, hex)
		hex2[len(hex)] = i2
		return fullKeys(n.child2, hex2, fullKeys(n.child1, hex1, fk))
	case *fullNode:
		for i, child := range n.Children {
			if child != nil {
				fk = fullKeys(child, concat(hex, byte(i)), fk)
			}
		}
		return fk
	case *accountNode:
		return append(fullKeys(n.storage, hex, fk), string(concat(hex, 16)))
	default:
		panic(fmt.Sprintf("%T", nd))
	}
}
