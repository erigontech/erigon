package iavl

import (
	"bytes"
	"fmt"
	"strings"

	cmn "github.com/tendermint/tendermint/libs/common"
)

// pathWithLeaf is a path to a leaf node and the leaf node itself.
type pathWithLeaf struct {
	Path PathToLeaf    `json:"path"`
	Leaf proofLeafNode `json:"leaf"`
}

func (pwl pathWithLeaf) String() string {
	return pwl.StringIndented("")
}

func (pwl pathWithLeaf) StringIndented(indent string) string {
	return fmt.Sprintf(`pathWithLeaf{
%s  Path: %v
%s  Leaf: %v
%s}`,
		indent, pwl.Path.stringIndented(indent+"  "),
		indent, pwl.Leaf.stringIndented(indent+"  "),
		indent)
}

// `verify` checks that the leaf node's hash + the inner nodes merkle-izes to
// the given root. If it returns an error, it means the leafHash or the
// PathToLeaf is incorrect.
func (pwl pathWithLeaf) verify(root []byte) cmn.Error {
	leafHash := pwl.Leaf.Hash()
	return pwl.Path.verify(leafHash, root)
}

// `computeRootHash` computes the root hash with leaf node.
// Does not verify the root hash.
func (pwl pathWithLeaf) computeRootHash() []byte {
	leafHash := pwl.Leaf.Hash()
	return pwl.Path.computeRootHash(leafHash)
}

//----------------------------------------

// PathToLeaf represents an inner path to a leaf node.
// Note that the nodes are ordered such that the last one is closest
// to the root of the tree.
type PathToLeaf []proofInnerNode

func (pl PathToLeaf) String() string {
	return pl.stringIndented("")
}

func (pl PathToLeaf) stringIndented(indent string) string {
	if len(pl) == 0 {
		return "empty-PathToLeaf"
	}
	strs := make([]string, len(pl))
	for i, pin := range pl {
		if i == 20 {
			strs[i] = fmt.Sprintf("... (%v total)", len(pl))
			break
		}
		strs[i] = fmt.Sprintf("%v:%v", i, pin.stringIndented(indent+"  "))
	}
	return fmt.Sprintf(`PathToLeaf{
%s  %v
%s}`,
		indent, strings.Join(strs, "\n"+indent+"  "),
		indent)
}

// `verify` checks that the leaf node's hash + the inner nodes merkle-izes to
// the given root. If it returns an error, it means the leafHash or the
// PathToLeaf is incorrect.
func (pl PathToLeaf) verify(leafHash []byte, root []byte) cmn.Error {
	hash := leafHash
	for i := len(pl) - 1; i >= 0; i-- {
		pin := pl[i]
		hash = pin.Hash(hash)
	}
	if !bytes.Equal(root, hash) {
		return cmn.ErrorWrap(ErrInvalidProof, "")
	}
	return nil
}

// `computeRootHash` computes the root hash assuming some leaf hash.
// Does not verify the root hash.
func (pl PathToLeaf) computeRootHash(leafHash []byte) []byte {
	hash := leafHash
	for i := len(pl) - 1; i >= 0; i-- {
		pin := pl[i]
		hash = pin.Hash(hash)
	}
	return hash
}

func (pl PathToLeaf) isLeftmost() bool {
	for _, node := range pl {
		if len(node.Left) > 0 {
			return false
		}
	}
	return true
}

func (pl PathToLeaf) isRightmost() bool {
	for _, node := range pl {
		if len(node.Right) > 0 {
			return false
		}
	}
	return true
}

func (pl PathToLeaf) isEmpty() bool {
	return pl == nil || len(pl) == 0
}

func (pl PathToLeaf) dropRoot() PathToLeaf {
	if pl.isEmpty() {
		return pl
	}
	return PathToLeaf(pl[:len(pl)-1])
}

func (pl PathToLeaf) hasCommonRoot(pl2 PathToLeaf) bool {
	if pl.isEmpty() || pl2.isEmpty() {
		return false
	}
	leftEnd := pl[len(pl)-1]
	rightEnd := pl2[len(pl2)-1]

	return bytes.Equal(leftEnd.Left, rightEnd.Left) &&
		bytes.Equal(leftEnd.Right, rightEnd.Right)
}

func (pl PathToLeaf) isLeftAdjacentTo(pl2 PathToLeaf) bool {
	for pl.hasCommonRoot(pl2) {
		pl, pl2 = pl.dropRoot(), pl2.dropRoot()
	}
	pl, pl2 = pl.dropRoot(), pl2.dropRoot()

	return pl.isRightmost() && pl2.isLeftmost()
}

// returns -1 if invalid.
func (pl PathToLeaf) Index() (idx int64) {
	for i, node := range pl {
		if node.Left == nil {
			continue
		} else if node.Right == nil {
			if i < len(pl)-1 {
				idx += node.Size - pl[i+1].Size
			} else {
				idx += node.Size - 1
			}
		} else {
			return -1
		}
	}
	return idx
}
