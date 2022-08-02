package iavl

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/tendermint/tendermint/crypto/tmhash"
	cmn "github.com/tendermint/tendermint/libs/common"
)

type RangeProof struct {
	// You don't need the right path because
	// it can be derived from what we have.
	LeftPath   PathToLeaf      `json:"left_path"`
	InnerNodes []PathToLeaf    `json:"inner_nodes"`
	Leaves     []proofLeafNode `json:"leaves"`

	// memoize
	rootVerified bool
	rootHash     []byte // valid iff rootVerified is true
	treeEnd      bool   // valid iff rootVerified is true

}

// Keys returns all the keys in the RangeProof.  NOTE: The keys here may
// include more keys than provided by tree.GetRangeWithProof or
// MutableTree.GetVersionedRangeWithProof.  The keys returned there are only
// in the provided [startKey,endKey){limit} range.  The keys returned here may
// include extra keys, such as:
// - the key before startKey if startKey is provided and doesn't exist;
// - the key after a queried key with tree.GetWithProof, when the key is absent.
func (proof *RangeProof) Keys() (keys [][]byte) {
	if proof == nil {
		return nil
	}
	for _, leaf := range proof.Leaves {
		keys = append(keys, leaf.Key)
	}
	return keys
}

// String returns a string representation of the proof.
func (proof *RangeProof) String() string {
	if proof == nil {
		return "<nil-RangeProof>"
	}
	return proof.StringIndented("")
}

func (proof *RangeProof) StringIndented(indent string) string {
	istrs := make([]string, 0, len(proof.InnerNodes))
	for _, ptl := range proof.InnerNodes {
		istrs = append(istrs, ptl.stringIndented(indent+"    "))
	}
	lstrs := make([]string, 0, len(proof.Leaves))
	for _, leaf := range proof.Leaves {
		lstrs = append(lstrs, leaf.stringIndented(indent+"    "))
	}
	return fmt.Sprintf(`RangeProof{
%s  LeftPath: %v
%s  InnerNodes:
%s    %v
%s  Leaves:
%s    %v
%s  (rootVerified): %v
%s  (rootHash): %X
%s  (treeEnd): %v
%s}`,
		indent, proof.LeftPath.stringIndented(indent+"  "),
		indent,
		indent, strings.Join(istrs, "\n"+indent+"    "),
		indent,
		indent, strings.Join(lstrs, "\n"+indent+"    "),
		indent, proof.rootVerified,
		indent, proof.rootHash,
		indent, proof.treeEnd,
		indent)
}

// The index of the first leaf (of the whole tree).
// Returns -1 if the proof is nil.
func (proof *RangeProof) LeftIndex() int64 {
	if proof == nil {
		return -1
	}
	return proof.LeftPath.Index()
}

// Also see LeftIndex().
// Verify that a key has some value.
// Does not assume that the proof itself is valid, call Verify() first.
func (proof *RangeProof) VerifyItem(key, value []byte) error {
	leaves := proof.Leaves
	if proof == nil {
		return cmn.ErrorWrap(ErrInvalidProof, "proof is nil")
	}
	if !proof.rootVerified {
		return cmn.NewError("must call Verify(root) first.")
	}
	i := sort.Search(len(leaves), func(i int) bool {
		return bytes.Compare(key, leaves[i].Key) <= 0
	})
	if i >= len(leaves) || !bytes.Equal(leaves[i].Key, key) {
		return cmn.ErrorWrap(ErrInvalidProof, "leaf key not found in proof")
	}
	valueHash := tmhash.Sum(value)
	if !bytes.Equal(leaves[i].ValueHash, valueHash) {
		return cmn.ErrorWrap(ErrInvalidProof, "leaf value hash not same")
	}
	return nil
}

// Verify that proof is valid absence proof for key.
// Does not assume that the proof itself is valid.
// For that, use Verify(root).
func (proof *RangeProof) VerifyAbsence(key []byte) error {
	if proof == nil {
		return cmn.ErrorWrap(ErrInvalidProof, "proof is nil")
	}
	if !proof.rootVerified {
		return cmn.NewError("must call Verify(root) first.")
	}
	cmp := bytes.Compare(key, proof.Leaves[0].Key)
	if cmp < 0 {
		if proof.LeftPath.isLeftmost() {
			return nil
		} else {
			return cmn.NewError("absence not proved by left path")
		}
	} else if cmp == 0 {
		return cmn.NewError("absence disproved via first item #0")
	}
	if len(proof.LeftPath) == 0 {
		return nil // proof ok
	}
	if proof.LeftPath.isRightmost() {
		return nil
	}

	// See if any of the leaves are greater than key.
	for i := 1; i < len(proof.Leaves); i++ {
		leaf := proof.Leaves[i]
		cmp := bytes.Compare(key, leaf.Key)
		if cmp < 0 {
			return nil // proof ok
		} else if cmp == 0 {
			return cmn.NewError("absence disproved via item #%v", i)
		}
	}

	// It's still a valid proof if our last leaf is the rightmost child.
	if proof.treeEnd {
		return nil // OK!
	}

	// It's not a valid absence proof.
	if len(proof.Leaves) < 2 {
		return cmn.NewError("absence not proved by right leaf (need another leaf?)")
	} else {
		return cmn.NewError("absence not proved by right leaf")
	}
}

// Verify that proof is valid.
func (proof *RangeProof) Verify(root []byte) error {
	if proof == nil {
		return cmn.ErrorWrap(ErrInvalidProof, "proof is nil")
	}
	err := proof.verify(root)
	return err
}

func (proof *RangeProof) verify(root []byte) (err error) {
	rootHash := proof.rootHash
	if rootHash == nil {
		derivedHash, err := proof.computeRootHash()
		if err != nil {
			return err
		}
		rootHash = derivedHash
	}
	if !bytes.Equal(rootHash, root) {
		return cmn.ErrorWrap(ErrInvalidRoot, "root hash doesn't match")
	} else {
		proof.rootVerified = true
	}
	return nil
}

// ComputeRootHash computes the root hash with leaves.
// Returns nil if error or proof is nil.
// Does not verify the root hash.
func (proof *RangeProof) ComputeRootHash() []byte {
	if proof == nil {
		return nil
	}
	rootHash, _ := proof.computeRootHash()
	return rootHash
}

func (proof *RangeProof) computeRootHash() (rootHash []byte, err error) {
	rootHash, treeEnd, err := proof._computeRootHash()
	if err == nil {
		proof.rootHash = rootHash // memoize
		proof.treeEnd = treeEnd   // memoize
	}
	return rootHash, err
}

func (proof *RangeProof) _computeRootHash() (rootHash []byte, treeEnd bool, err error) {
	if len(proof.Leaves) == 0 {
		return nil, false, cmn.ErrorWrap(ErrInvalidProof, "no leaves")
	}
	if len(proof.InnerNodes)+1 != len(proof.Leaves) {
		return nil, false, cmn.ErrorWrap(ErrInvalidProof, "InnerNodes vs Leaves length mismatch, leaves should be 1 more.")
	}

	// Start from the left path and prove each leaf.

	// shared across recursive calls
	var leaves = proof.Leaves
	var innersq = proof.InnerNodes
	var COMPUTEHASH func(path PathToLeaf, rightmost bool) (hash []byte, treeEnd bool, done bool, err error)

	// rightmost: is the root a rightmost child of the tree?
	// treeEnd: true iff the last leaf is the last item of the tree.
	// Returns the (possibly intermediate, possibly root) hash.
	COMPUTEHASH = func(path PathToLeaf, rightmost bool) (hash []byte, treeEnd bool, done bool, err error) {

		// Pop next leaf.
		nleaf, rleaves := leaves[0], leaves[1:]
		leaves = rleaves

		// Compute hash.
		hash = (pathWithLeaf{
			Path: path,
			Leaf: nleaf,
		}).computeRootHash()

		// If we don't have any leaves left, we're done.
		if len(leaves) == 0 {
			rightmost = rightmost && path.isRightmost()
			return hash, rightmost, true, nil
		}

		// Prove along path (until we run out of leaves).
		for len(path) > 0 {

			// Drop the leaf-most (last-most) inner nodes from path
			// until we encounter one with a left hash.
			// We assume that the left side is already verified.
			// rpath: rest of path
			// lpath: last path item
			rpath, lpath := path[:len(path)-1], path[len(path)-1]
			path = rpath
			if len(lpath.Right) == 0 {
				continue
			}

			// Pop next inners, a PathToLeaf (e.g. []proofInnerNode).
			inners, rinnersq := innersq[0], innersq[1:]
			innersq = rinnersq

			// Recursively verify inners against remaining leaves.
			derivedRoot, treeEnd, done, err := COMPUTEHASH(inners, rightmost && rpath.isRightmost())
			if err != nil {
				return nil, treeEnd, false, cmn.ErrorWrap(err, "recursive COMPUTEHASH call")
			}
			if !bytes.Equal(derivedRoot, lpath.Right) {
				return nil, treeEnd, false, cmn.ErrorWrap(ErrInvalidRoot, "intermediate root hash %X doesn't match, got %X", lpath.Right, derivedRoot)
			}
			if done {
				return hash, treeEnd, true, nil
			}
		}

		// We're not done yet (leaves left over). No error, not done either.
		// Technically if rightmost, we know there's an error "left over leaves
		// -- malformed proof", but we return that at the top level, below.
		return hash, false, false, nil
	}

	// Verify!
	path := proof.LeftPath
	rootHash, treeEnd, done, err := COMPUTEHASH(path, true)
	if err != nil {
		return nil, treeEnd, cmn.ErrorWrap(err, "root COMPUTEHASH call")
	} else if !done {
		return nil, treeEnd, cmn.ErrorWrap(ErrInvalidProof, "left over leaves -- malformed proof")
	}

	// Ok!
	return rootHash, treeEnd, nil
}

///////////////////////////////////////////////////////////////////////////////
