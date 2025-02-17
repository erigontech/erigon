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

package optimistic

import (
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
)

type optimisticStoreImpl struct {
	opMutex         sync.RWMutex
	optimisticRoots sync.Map //map[common.Hash]*opNode
}

func NewOptimisticStore() OptimisticStore {
	return &optimisticStoreImpl{}
}

type opNode struct {
	execBlockNum uint64
	parent       common.Hash
	children     []common.Hash
}

func (impl *optimisticStoreImpl) AddOptimisticCandidate(block *cltypes.BeaconBlock) error {
	if block.Body.ExecutionPayload == nil || *block.Body.ExecutionPayload == (cltypes.Eth1Block{}) {
		return nil
	}

	root := block.StateRoot
	parentRoot := block.ParentRoot
	impl.opMutex.Lock()
	defer impl.opMutex.Unlock()

	if _, ok := impl.optimisticRoots.Load(root); ok {
		// block already optimistically imported
		return nil
	}
	blockNode := &opNode{
		execBlockNum: block.Body.ExecutionPayload.BlockNumber,
		parent:       parentRoot,
		children:     []common.Hash{},
	}
	impl.optimisticRoots.Store(root, blockNode)

	// check if parent is already in the store
	// if _, ok := impl.optimisticRoots[parentRoot]; ok {
	// 	impl.optimisticRoots[parentRoot].children = append(impl.optimisticRoots[parentRoot].children, root)
	// }
	if parent, ok := impl.optimisticRoots.Load(parentRoot); ok {
		parent.(*opNode).children = append(parent.(*opNode).children, root)
		impl.optimisticRoots.Store(parentRoot, parent)
	}
	return nil
}

func (impl *optimisticStoreImpl) ValidateBlock(block *cltypes.BeaconBlock) error {
	// When a block transitions from NOT_VALIDATED -> VALID, all ancestors of the block MUST also transition
	// from NOT_VALIDATED -> VALID. Such a block and any previously NOT_VALIDATED ancestors are no longer considered "optimistically imported".
	if block.Body.ExecutionPayload == nil || *block.Body.ExecutionPayload == (cltypes.Eth1Block{}) {
		return nil
	}
	blockNum := block.Body.ExecutionPayload.BlockNumber
	impl.opMutex.Lock()
	defer impl.opMutex.Unlock()
	curRoot := block.StateRoot
	for {
		if node, ok := impl.optimisticRoots.Load(curRoot); ok {
			// validate the block
			// remove the block from the store
			impl.optimisticRoots.Delete(curRoot)
			curRoot = node.(*opNode).parent
		} else {
			break
		}
	}
	// and try to clean up all nodes with block number less than blockNum
	toRemoves := []common.Hash{}
	// for root, node := range impl.optimisticRoots {
	// 	if node.execBlockNum < blockNum {
	// 		toRemoves = append(toRemoves, root)
	// 	}
	// }
	// for _, root := range toRemoves {
	// 	delete(impl.optimisticRoots, root)
	// }
	impl.optimisticRoots.Range(func(root, node interface{}) bool {
		if node.(*opNode).execBlockNum < blockNum {
			toRemoves = append(toRemoves, root.(common.Hash))
		}
		return true
	})
	for _, root := range toRemoves {
		impl.optimisticRoots.Delete(root)
	}
	return nil
}

func (impl *optimisticStoreImpl) InvalidateBlock(block *cltypes.BeaconBlock) error {
	// When a block transitions from NOT_VALIDATED -> INVALIDATED, all descendants of the block MUST also transition
	// from NOT_VALIDATED -> INVALIDATED.
	if block.Body.ExecutionPayload == nil || *block.Body.ExecutionPayload == (cltypes.Eth1Block{}) {
		return nil
	}
	impl.opMutex.Lock()
	defer impl.opMutex.Unlock()
	// start from the block to be invalidated, and remove all its descendants
	toRemoves := []common.Hash{block.StateRoot}
	for len(toRemoves) > 0 {
		curRoot := toRemoves[0]
		toRemoves = toRemoves[1:]
		if node, ok := impl.optimisticRoots.Load(curRoot); ok {
			// remove the invalidated block from the store
			impl.optimisticRoots.Delete(curRoot)
			toRemoves = append(toRemoves, node.(*opNode).children...)
		}
	}
	return nil
}

func (impl *optimisticStoreImpl) IsOptimistic(root common.Hash) bool {
	if root == (common.Hash{}) {
		return false
	}
	if _, ok := impl.optimisticRoots.Load(root); ok {
		return true
	}
	return false
}
