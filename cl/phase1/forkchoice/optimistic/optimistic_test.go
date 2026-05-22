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
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/suite"
)

var (
	mockBlock1Root = common.Hash{11}
	mockBlock2Root = common.Hash{12}
	mockBlock3Root = common.Hash{13}
	mockBlock4Root = common.Hash{14}

	// Mock blocks for testing
	// mock1 -> mock2 -> mock3_1
	// 	 			  -> mock3_2
	mockBlock1 = &cltypes.BeaconBlock{
		StateRoot:  common.Hash{1},
		ParentRoot: common.Hash{0},
		Body: &cltypes.BeaconBody{
			ExecutionPayload: &cltypes.Eth1Block{
				BlockNumber: 1,
			},
		},
	}
	mockBlock2 = &cltypes.BeaconBlock{
		StateRoot:  common.Hash{2},
		ParentRoot: mockBlock1Root,
		Body: &cltypes.BeaconBody{
			ExecutionPayload: &cltypes.Eth1Block{
				BlockNumber: 2,
			},
		},
	}
	mockBlock3_1 = &cltypes.BeaconBlock{
		StateRoot:  common.Hash{3},
		ParentRoot: mockBlock2Root,
		Body: &cltypes.BeaconBody{
			ExecutionPayload: &cltypes.Eth1Block{
				BlockNumber: 3,
			},
		},
	}
	mockBlock3_2 = &cltypes.BeaconBlock{
		StateRoot:  common.Hash{4},
		ParentRoot: mockBlock2Root,
		Body: &cltypes.BeaconBody{
			ExecutionPayload: &cltypes.Eth1Block{
				BlockNumber: 3,
			},
		},
	}
)

type optimisticTestSuite struct {
	suite.Suite
	opStore *optimisticStoreImpl
}

func (t *optimisticTestSuite) SetupTest() {
	t.opStore = NewOptimisticStore().(*optimisticStoreImpl)
}

func (t *optimisticTestSuite) TearDownTest() {
}

func checkSyncMapLength(m *sync.Map, length int) bool {
	l := 0
	m.Range(func(_, _ any) bool {
		l++
		return true
	})
	return l == length
}

func (t *optimisticTestSuite) TestAddOptimisticCandidate() {

	// Add an optimistic candidate
	err := t.opStore.AddOptimisticCandidate(mockBlock1Root, mockBlock1)
	t.Require().NoError(err)
	// Add the same optimistic candidate again, expect nothing to happen
	err = t.opStore.AddOptimisticCandidate(mockBlock1Root, mockBlock1)
	t.Require().NoError(err)
	// Check optimisticRoots table
	t.Require().True(checkSyncMapLength(&t.opStore.optimisticRoots, 1))
	node, ok := t.opStore.optimisticRoots.Load(mockBlock1Root)
	t.Require().True(ok)
	t.Require().Equal(&opNode{
		execBlockNum: mockBlock1.Body.ExecutionPayload.BlockNumber,
		parent:       mockBlock1.ParentRoot,
		children:     []common.Hash{},
	}, node)

	// Add a child block
	err = t.opStore.AddOptimisticCandidate(mockBlock2Root, mockBlock2)
	t.Require().NoError(err)
	// check connection between parent and child
	t.Require().True(checkSyncMapLength(&t.opStore.optimisticRoots, 2))
	node, ok = t.opStore.optimisticRoots.Load(mockBlock1Root)
	t.Require().True(ok)
	t.Require().Equal(&opNode{
		execBlockNum: 1,
		parent:       common.Hash{0},
		children:     []common.Hash{mockBlock2Root},
	}, node)
	node, ok = t.opStore.optimisticRoots.Load(mockBlock2Root)
	t.Require().True(ok)
	t.Require().Equal(&opNode{
		execBlockNum: mockBlock2.Body.ExecutionPayload.BlockNumber,
		parent:       mockBlock2.ParentRoot,
		children:     []common.Hash{},
	}, node)
}

func (t *optimisticTestSuite) TestValidateBlock() {
	for _, blockWithRoot := range []struct {
		root  common.Hash
		block *cltypes.BeaconBlock
	}{
		{root: mockBlock1Root, block: mockBlock1},
		{root: mockBlock2Root, block: mockBlock2},
		{root: mockBlock3Root, block: mockBlock3_1},
		{root: mockBlock4Root, block: mockBlock3_2},
	} {
		err := t.opStore.AddOptimisticCandidate(blockWithRoot.root, blockWithRoot.block)
		t.Require().NoError(err)
	}

	// Validate the last block
	err := t.opStore.ValidateBlock(mockBlock4Root, mockBlock3_2)
	t.Require().NoError(err)
	// Check optimisticRoots table
	t.Require().True(checkSyncMapLength(&t.opStore.optimisticRoots, 1))
	node, ok := t.opStore.optimisticRoots.Load(mockBlock3Root)
	t.Require().True(ok)
	t.Require().Equal(&opNode{
		execBlockNum: mockBlock3_1.Body.ExecutionPayload.BlockNumber,
		parent:       mockBlock3_1.ParentRoot,
		children:     []common.Hash{},
	}, node)
}

func (t *optimisticTestSuite) TestInvalidateBlock() {
	for _, blockWithRoot := range []struct {
		root  common.Hash
		block *cltypes.BeaconBlock
	}{
		{root: mockBlock1Root, block: mockBlock1},
		{root: mockBlock2Root, block: mockBlock2},
		{root: mockBlock3Root, block: mockBlock3_1},
		{root: mockBlock4Root, block: mockBlock3_2},
	} {
		err := t.opStore.AddOptimisticCandidate(blockWithRoot.root, blockWithRoot.block)
		t.Require().NoError(err)
	}

	// Invalidate the first block
	err := t.opStore.InvalidateBlock(mockBlock1Root, mockBlock1)
	t.Require().NoError(err)
	// Check optimisticRoots table
	t.Require().True(checkSyncMapLength(&t.opStore.optimisticRoots, 0))
}

func TestOptimistic(t *testing.T) {
	suite.Run(t, new(optimisticTestSuite))
}
