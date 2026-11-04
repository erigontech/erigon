// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Debugging utilities for Merkle Patricia trees

package trie

import (
	"encoding/hex"
	"fmt"
	"strings"
)

func (n *FullNode) fstring(ind string) string {
	var resp strings.Builder
	resp.WriteString(fmt.Sprintf("full\n%s  ", ind))
	for i, node := range &n.Children {
		if node == nil {
			resp.WriteString(indices[i] + ": <nil> ")
		} else {
			resp.WriteString(indices[i] + ": " + node.fstring(ind+"  "))
		}
	}
	return resp.String() + "\n" + ind + "]"
}

func (n *DuoNode) fstring(ind string) string {
	var resp strings.Builder
	resp.WriteString(fmt.Sprintf("duo[\n%s  ", ind))
	i1, i2 := n.childrenIdx()
	resp.WriteString(fmt.Sprintf("%s: %v", indices[i1], n.child1.fstring(ind+"  ")))
	resp.WriteString(fmt.Sprintf("%s: %v", indices[i2], n.child2.fstring(ind+"  ")))
	resp.WriteString(fmt.Sprintf("\n%s] ", ind))
	return resp.String()
}

func (n *ShortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: %v} ", n.Key, n.Val.fstring(ind+"  "))
}

func (n HashNode) fstring(ind string) string {
	return fmt.Sprintf("<%x> ", n.hash)
}

func (n ValueNode) fstring(ind string) string {
	return fmt.Sprintf("%x ", []byte(n))
}

func (n CodeNode) fstring(ind string) string {
	return fmt.Sprintf("code: %x ", []byte(n))
}

func (an AccountNode) fstring(ind string) string {
	encodedAccount := make([]byte, an.EncodingLengthForHashing())
	an.EncodeForHashing(encodedAccount)
	if an.Storage == nil {
		return hex.EncodeToString(encodedAccount)
	}
	return hex.EncodeToString(encodedAccount) + " " + an.Storage.fstring(ind+" ")
}
