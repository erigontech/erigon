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

package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor"
)

type producerSlotIndex uint64

type forkTreeNode struct {
	parent          *forkTreeNode
	children        map[producerSlotIndex]*forkTreeNode
	header          *types.Header
	headerHash      common.Hash
	totalDifficulty uint64
}

type difficultyCalculator interface {
	HeaderDifficulty(ctx context.Context, header *types.Header) (uint64, error)
}

type headerValidator interface {
	ValidateHeader(ctx context.Context, header *types.Header, parent *types.Header, now time.Time) error
}

func NewCanonicalChainBuilder(root *types.Header, dc difficultyCalculator, hv headerValidator) *CanonicalChainBuilder {
	ccb := &CanonicalChainBuilder{
		difficultyCalc:  dc,
		headerValidator: hv,
	}
	ccb.Reset(root)
	return ccb
}

type CanonicalChainBuilder struct {
	root            *forkTreeNode
	tip             *forkTreeNode
	difficultyCalc  difficultyCalculator
	headerValidator headerValidator
}

func (ccb *CanonicalChainBuilder) Reset(root *types.Header) {
	ccb.root = &forkTreeNode{
		children:   make(map[producerSlotIndex]*forkTreeNode),
		header:     root,
		headerHash: root.Hash(),
	}
	ccb.tip = ccb.root
}

// depth-first search
func (ccb *CanonicalChainBuilder) enumerate(visitFunc func(*forkTreeNode) bool) {
	stack := []*forkTreeNode{ccb.root}
	for len(stack) > 0 {
		// pop
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if !visitFunc(node) {
			break
		}

		for _, child := range node.children {
			stack = append(stack, child)
		}
	}
}

func (ccb *CanonicalChainBuilder) nodeByHash(hash common.Hash) *forkTreeNode {
	var result *forkTreeNode
	ccb.enumerate(func(node *forkTreeNode) bool {
		if node.headerHash == hash {
			result = node
		}
		return result == nil
	})
	return result
}

func (ccb *CanonicalChainBuilder) ContainsHash(hash common.Hash) bool {
	return ccb.nodeByHash(hash) != nil
}

func (ccb *CanonicalChainBuilder) Tip() *types.Header {
	return ccb.tip.header
}
func (ccb *CanonicalChainBuilder) Root() *types.Header {
	return ccb.root.header
}

func (ccb *CanonicalChainBuilder) Headers() []*types.Header {
	var headers []*types.Header
	node := ccb.tip
	for node != nil {
		headers = append(headers, node.header)
		node = node.parent
	}
	slices.Reverse(headers)
	return headers
}

func (ccb *CanonicalChainBuilder) HeadersInRange(start uint64, count uint64) []*types.Header {
	headers := ccb.Headers()
	if len(headers) == 0 {
		return nil
	}
	if headers[0].Number.Uint64() > start {
		return nil
	}
	if headers[len(headers)-1].Number.Uint64() < start+count-1 {
		return nil
	}

	offset := start - headers[0].Number.Uint64()
	return headers[offset : offset+count]
}

func (ccb *CanonicalChainBuilder) PruneRoot(newRootNum uint64) error {
	if (newRootNum < ccb.root.header.Number.Uint64()) || (newRootNum > ccb.Tip().Number.Uint64()) {
		return errors.New("CanonicalChainBuilder.PruneRoot: newRootNum outside of the canonical chain")
	}

	newRoot := ccb.tip
	for newRoot.header.Number.Uint64() > newRootNum {
		newRoot = newRoot.parent
	}

	ccb.root = newRoot
	return nil
}

func (ccb *CanonicalChainBuilder) PruneNode(hash common.Hash) error {
	if ccb.root.headerHash == hash {
		return errors.New("CanonicalChainBuilder.PruneNode: can't prune root node")
	}

	var exists bool
	ccb.enumerate(func(node *forkTreeNode) bool {
		if node.headerHash != hash {
			return true
		}

		for idx, parentChild := range node.parent.children {
			if parentChild.headerHash == hash {
				exists = true
				delete(node.parent.children, idx)
				break
			}
		}

		return false
	})
	if !exists {
		return errors.New("CanonicalChainBuilder.PruneNode: could not find node to prune")
	}

	ccb.tip = ccb.recalcTip() // tip may have changed after prunning, re-calc
	return nil
}

// compareForkTreeNodes compares 2 fork tree nodes.
// It returns a positive number if the chain ending at node1 is "better" than the chain ending at node2.
// The better node belongs to the canonical chain, and it has:
// * a greater total difficulty,
// * or a smaller block number,
// * or a lexicographically greater hash.
// See: https://github.com/maticnetwork/bor/blob/master/core/forkchoice.go#L82
func compareForkTreeNodes(node1 *forkTreeNode, node2 *forkTreeNode) int {
	difficultyDiff := int64(node1.totalDifficulty) - int64(node2.totalDifficulty)
	if difficultyDiff != 0 {
		return int(difficultyDiff)
	}
	blockNumDiff := node1.header.Number.Cmp(node2.header.Number)
	if blockNumDiff != 0 {
		return -blockNumDiff
	}
	return bytes.Compare(node1.headerHash.Bytes(), node2.headerHash.Bytes())
}

func (ccb *CanonicalChainBuilder) updateTipIfNeeded(tipCandidate *forkTreeNode) {
	if compareForkTreeNodes(tipCandidate, ccb.tip) > 0 {
		ccb.tip = tipCandidate
	}
}

func (ccb *CanonicalChainBuilder) recalcTip() *forkTreeNode {
	var tip *forkTreeNode
	ccb.enumerate(func(node *forkTreeNode) bool {
		if tip == nil {
			tip = node
			return true
		}

		if compareForkTreeNodes(tip, node) < 0 {
			tip = node
		}

		return true
	})
	return tip
}

// Connect connects a list of headers to the canonical chain builder tree.
// Returns the list of newly connected headers (filtering out headers that already exist in the tree)
// or an error in case the header is invalid or the header chain cannot reach any of the nodes in the tree.
func (ccb *CanonicalChainBuilder) Connect(ctx context.Context, headers []*types.Header) ([]*types.Header, error) {
	if (len(headers) > 0) && (headers[0].Number != nil) && (headers[0].Number.Cmp(ccb.root.header.Number) == 0) {
		headers = headers[1:]
	}
	if len(headers) == 0 {
		return nil, nil
	}

	parent := ccb.nodeByHash(headers[0].ParentHash)
	if parent == nil {
		return nil, errors.New("CanonicalChainBuilder.Connect: can't connect headers")
	}

	headersHashes := common.SliceMap(headers, func(header *types.Header) common.Hash {
		return header.Hash()
	})

	// check if headers are linked by ParentHash
	for i, header := range headers[1:] {
		if header.ParentHash != headersHashes[i] {
			return nil, errors.New("CanonicalChainBuilder.Connect: invalid headers slice ParentHash")
		}
	}

	// skip existing matching nodes until a new header is found
	for len(headers) > 0 {
		var matchingNode *forkTreeNode
		for _, c := range parent.children {
			if c.headerHash == headersHashes[0] {
				matchingNode = c
				break
			}
		}
		if matchingNode != nil {
			parent = matchingNode
			headers = headers[1:]
			headersHashes = headersHashes[1:]
		} else {
			break
		}
	}

	// if all headers are already inserted
	if len(headers) == 0 {
		return nil, nil
	}

	// attach nodes for the new headers
	for i, header := range headers {
		if (header.Number == nil) || (header.Number.Uint64() != parent.header.Number.Uint64()+1) {
			return nil, errors.New("CanonicalChainBuilder.Connect: invalid header.Number")
		}

		if err := ccb.headerValidator.ValidateHeader(ctx, header, parent.header, time.Now()); err != nil {
			return nil, fmt.Errorf("CanonicalChainBuilder.Connect: invalid header error %w", err)
		}

		difficulty, err := ccb.difficultyCalc.HeaderDifficulty(ctx, header)
		if err != nil {
			return nil, fmt.Errorf("CanonicalChainBuilder.Connect: header difficulty error %w", err)
		}
		if (header.Difficulty == nil) || (header.Difficulty.Uint64() != difficulty) {
			err := &bor.WrongDifficultyError{
				Number:   header.Number.Uint64(),
				Expected: difficulty,
				Actual:   header.Difficulty.Uint64(),
				Signer:   []byte{},
			}
			return nil, err
		}

		slot := producerSlotIndex(difficulty)
		if _, ok := parent.children[slot]; ok {
			return nil, errors.New("CanonicalChainBuilder.Connect: producer slot is already filled by a different header")
		}

		node := &forkTreeNode{
			parent:          parent,
			children:        make(map[producerSlotIndex]*forkTreeNode),
			header:          header,
			headerHash:      headersHashes[i],
			totalDifficulty: parent.totalDifficulty + difficulty,
		}

		parent.children[slot] = node
		parent = node
		ccb.updateTipIfNeeded(node)
	}

	return headers, nil
}

func (ccb *CanonicalChainBuilder) LowestCommonAncestor(a, b common.Hash) (*types.Header, bool) {
	pathA := ccb.pathToRoot(a)
	if len(pathA) == 0 {
		// 'a' doesn't exist in the tree
		return nil, false
	}

	pathB := ccb.pathToRoot(b)
	if len(pathB) == 0 {
		// 'b' doesn't exist in the tree
		return nil, false
	}

	heightA := pathA[0].header.Number.Uint64()
	heightB := pathB[0].header.Number.Uint64()
	for heightA != heightB {
		if heightA < heightB {
			pathB = pathB[1:]
			heightB = pathB[0].header.Number.Uint64()
		} else if heightA > heightB {
			pathA = pathA[1:]
			heightA = pathA[0].header.Number.Uint64()
		}
	}

	for i := 0; i < len(pathA); i++ {
		if pathA[i].headerHash == pathB[i].headerHash {
			return pathA[i].header, true
		}
	}

	return nil, false
}

func (ccb *CanonicalChainBuilder) pathToRoot(from common.Hash) []*forkTreeNode {
	path := make([]*forkTreeNode, 0, ccb.Tip().Number.Uint64()-ccb.Root().Number.Uint64())
	pathToRootRec(ccb.root, from, &path)
	return path
}

func pathToRootRec(node *forkTreeNode, from common.Hash, path *[]*forkTreeNode) bool {
	if node.headerHash == from {
		*path = append(*path, node)
		return true
	}

	for _, child := range node.children {
		if pathToRootRec(child, from, path) {
			*path = append(*path, node)
			return true
		}
	}

	return false
}
