// Copyright 2026 The Erigon Authors
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

package bscsync

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

var ErrUnknownParent = errors.New("CanonicalChainBuilder.Connect: can't connect headers: unknown parent")

type HeaderVerifier interface {
	VerifyHeader(c parlia.Chain, h *types.Header) error
	ApplyHeader(r parlia.HeaderReader, snap *parlia.Snapshot, h *types.Header) (*parlia.Snapshot, error)
}

type parliaVerifier struct {
	*parlia.Verifier
	config *chain.Config
}

func NewParliaVerifier(config *chain.Config) HeaderVerifier {
	return parliaVerifier{Verifier: parlia.NewVerifier(config), config: config}
}

func (v parliaVerifier) ApplyHeader(r parlia.HeaderReader, snap *parlia.Snapshot, h *types.Header) (*parlia.Snapshot, error) {
	return snap.Apply(v.config, []*types.Header{h}, r)
}

type forkTreeNode struct {
	parent          *forkTreeNode
	children        map[common.Hash]*forkTreeNode
	header          *types.Header
	headerHash      common.Hash
	totalDifficulty uint64
	snapshot        *parlia.Snapshot
}

// CanonicalChainBuilder keeps an in-memory tree of verified headers rooted at a trusted
// header and tracks the best tip. Lookups below the root go to db.
type CanonicalChainBuilder struct {
	mu       sync.Mutex
	db       parlia.Chain
	verifier HeaderVerifier
	root     *forkTreeNode
	tip      *forkTreeNode
	nodes    map[common.Hash]*forkTreeNode
}

func NewCanonicalChainBuilder(db parlia.Chain, verifier HeaderVerifier, root *types.Header) (*CanonicalChainBuilder, error) {
	ccb := &CanonicalChainBuilder{db: db, verifier: verifier}
	if err := ccb.Reset(root); err != nil {
		return nil, err
	}
	return ccb, nil
}

func (ccb *CanonicalChainBuilder) Reset(root *types.Header) error {
	hash := root.Hash()
	snap, err := ccb.db.Snapshot(hash, root.Number.Uint64())
	if err != nil {
		return fmt.Errorf("CanonicalChainBuilder.Reset: root snapshot: %w", err)
	}
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	ccb.root = &forkTreeNode{
		children:   make(map[common.Hash]*forkTreeNode),
		header:     root,
		headerHash: hash,
		snapshot:   snap,
	}
	ccb.tip = ccb.root
	ccb.nodes = map[common.Hash]*forkTreeNode{hash: ccb.root}
	return nil
}

func (ccb *CanonicalChainBuilder) ContainsHash(hash common.Hash) bool {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	_, ok := ccb.nodes[hash]
	return ok
}

func (ccb *CanonicalChainBuilder) Tip() *types.Header {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	return ccb.tip.header
}

func (ccb *CanonicalChainBuilder) Root() *types.Header {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	return ccb.root.header
}

func (ccb *CanonicalChainBuilder) Headers() []*types.Header {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	var headers []*types.Header
	for node := ccb.tip; node != nil; node = node.parent {
		headers = append(headers, node.header)
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

func (ccb *CanonicalChainBuilder) HeaderReader() CcbHeaderReader {
	return CcbHeaderReader{ccb: ccb}
}

func (ccb *CanonicalChainBuilder) PruneRoot(newRootNum uint64) error {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	if (newRootNum < ccb.root.header.Number.Uint64()) || (newRootNum > ccb.tip.header.Number.Uint64()) {
		return errors.New("CanonicalChainBuilder.PruneRoot: newRootNum outside of the canonical chain")
	}

	newRoot := ccb.tip
	for newRoot.header.Number.Uint64() > newRootNum {
		newRoot = newRoot.parent
	}

	newRoot.parent = nil
	ccb.root = newRoot
	ccb.nodes = make(map[common.Hash]*forkTreeNode)
	ccb.walk(newRoot, func(node *forkTreeNode) { ccb.nodes[node.headerHash] = node })
	return nil
}

func (ccb *CanonicalChainBuilder) PruneNode(hash common.Hash) error {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	if ccb.root.headerHash == hash {
		return errors.New("CanonicalChainBuilder.PruneNode: can't prune root node")
	}
	node, ok := ccb.nodes[hash]
	if !ok {
		return errors.New("CanonicalChainBuilder.PruneNode: could not find node to prune")
	}

	delete(node.parent.children, hash)
	ccb.walk(node, func(n *forkTreeNode) { delete(ccb.nodes, n.headerHash) })
	ccb.tip = ccb.recalcTip()
	return nil
}

func (ccb *CanonicalChainBuilder) walk(from *forkTreeNode, visit func(*forkTreeNode)) {
	stack := []*forkTreeNode{from}
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		visit(node)
		for _, child := range node.children {
			stack = append(stack, child)
		}
	}
}

// compareForkTreeNodes returns a positive number if the chain ending at node1 is better than
// the chain ending at node2: a greater total difficulty, a smaller block number, or a
// lexicographically greater hash.
func compareForkTreeNodes(node1 *forkTreeNode, node2 *forkTreeNode) int {
	if c := cmp.Compare(node1.totalDifficulty, node2.totalDifficulty); c != 0 {
		return c
	}
	if c := node1.header.Number.Cmp(&node2.header.Number); c != 0 {
		return -c
	}
	return bytes.Compare(node1.headerHash[:], node2.headerHash[:])
}

func (ccb *CanonicalChainBuilder) updateTipIfNeeded(tipCandidate *forkTreeNode) {
	if compareForkTreeNodes(tipCandidate, ccb.tip) > 0 {
		ccb.tip = tipCandidate
	}
}

func (ccb *CanonicalChainBuilder) recalcTip() *forkTreeNode {
	tip := ccb.root
	ccb.walk(ccb.root, func(node *forkTreeNode) {
		if compareForkTreeNodes(tip, node) < 0 {
			tip = node
		}
	})
	return tip
}

// Connect verifies headers and attaches them to the tree. It returns the newly connected
// headers, also on error, because the tree has already been changed for those.
func (ccb *CanonicalChainBuilder) Connect(_ context.Context, headers []*types.Header) ([]*types.Header, error) {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	if len(headers) == 0 {
		return nil, nil
	}

	var isBehindRoot = func(h *types.Header) bool {
		return h.Number.Cmp(&ccb.root.header.Number) < 0
	}

	if isBehindRoot(headers[len(headers)-1]) {
		return nil, nil
	}
	var connectionIdx = 0
	if headers[0].Number.Cmp(&ccb.root.header.Number) <= 0 {
		for ; connectionIdx < len(headers) && isBehindRoot(headers[connectionIdx]); connectionIdx++ {
		}
		connectionIdx++
	}
	if connectionIdx >= len(headers) {
		return nil, nil
	}
	headers = headers[connectionIdx:]

	parent, ok := ccb.nodes[headers[0].ParentHash]
	if !ok {
		return nil, ErrUnknownParent
	}

	headersHashes := common.SliceMap(headers, func(header *types.Header) common.Hash {
		return header.Hash()
	})

	for i, header := range headers[1:] {
		if header.ParentHash != headersHashes[i] {
			return nil, errors.New("CanonicalChainBuilder.Connect: invalid headers slice ParentHash")
		}
	}

	for len(headers) > 0 {
		child, ok := parent.children[headersHashes[0]]
		if !ok {
			break
		}
		parent = child
		headers = headers[1:]
		headersHashes = headersHashes[1:]
	}
	if len(headers) == 0 {
		return nil, nil
	}

	c := ccb.chain()
	for i, header := range headers {
		processed := headers[:i]
		if header.Number.Uint64() != parent.header.Number.Uint64()+1 {
			return processed, fmt.Errorf("can't connect %d: invalid number: expected %d", header.Number.Uint64(), parent.header.Number.Uint64()+1)
		}
		if err := ccb.verifier.VerifyHeader(c, header); err != nil {
			return processed, fmt.Errorf("can't connect %d: invalid header: %w", header.Number.Uint64(), err)
		}
		snap, err := ccb.verifier.ApplyHeader(c, parent.snapshot, header)
		if err != nil {
			return processed, fmt.Errorf("can't connect %d: snapshot: %w", header.Number.Uint64(), err)
		}

		node := &forkTreeNode{
			parent:          parent,
			children:        make(map[common.Hash]*forkTreeNode),
			header:          header,
			headerHash:      headersHashes[i],
			totalDifficulty: parent.totalDifficulty + header.Difficulty.Uint64(),
			snapshot:        snap,
		}
		parent.children[node.headerHash] = node
		ccb.nodes[node.headerHash] = node
		parent = node
		ccb.updateTipIfNeeded(node)
	}

	return headers, nil
}

func (ccb *CanonicalChainBuilder) LowestCommonAncestor(a, b common.Hash) (*types.Header, bool) {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	nodeA, ok := ccb.nodes[a]
	if !ok {
		return nil, false
	}
	nodeB, ok := ccb.nodes[b]
	if !ok {
		return nil, false
	}
	for nodeA.header.Number.Uint64() > nodeB.header.Number.Uint64() {
		nodeA = nodeA.parent
	}
	for nodeB.header.Number.Uint64() > nodeA.header.Number.Uint64() {
		nodeB = nodeB.parent
	}
	for nodeA != nodeB {
		nodeA, nodeB = nodeA.parent, nodeB.parent
	}
	return nodeA.header, true
}

func (ccb *CanonicalChainBuilder) chain() ccbChain {
	return ccbChain{ccb: ccb}
}

// ccbChain resolves headers and snapshots from the tree first and from db below it. The
// caller must hold ccb.mu.
type ccbChain struct {
	ccb *CanonicalChainBuilder
}

func (c ccbChain) GetHeader(hash common.Hash, number uint64) *types.Header {
	if node, ok := c.ccb.nodes[hash]; ok && node.header.Number.Uint64() == number {
		return node.header
	}
	return c.ccb.db.GetHeader(hash, number)
}

func (c ccbChain) Snapshot(hash common.Hash, number uint64) (*parlia.Snapshot, error) {
	if node, ok := c.ccb.nodes[hash]; ok && node.header.Number.Uint64() == number {
		return node.snapshot, nil
	}
	return c.ccb.db.Snapshot(hash, number)
}

type CcbHeaderReader struct {
	ccb *CanonicalChainBuilder
}

func (r CcbHeaderReader) HeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error) {
	r.ccb.mu.Lock()
	defer r.ccb.mu.Unlock()
	node, ok := r.ccb.nodes[hash]
	if !ok {
		return nil, nil
	}
	return node.header, nil
}
