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

package builder

import (
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

const recentBlockBuiltCapacity = 16

type LatestBlockBuiltStore struct {
	block  *types.Block
	blocks map[common.Hash]*types.Block
	order  []common.Hash

	lock sync.RWMutex
}

func NewLatestBlockBuiltStore() *LatestBlockBuiltStore {
	return &LatestBlockBuiltStore{
		blocks: make(map[common.Hash]*types.Block),
	}
}

func (s *LatestBlockBuiltStore) AddBlockBuilt(block *types.Block) {
	if block == nil {
		return
	}

	hash := block.Hash()

	s.lock.Lock()
	defer s.lock.Unlock()

	s.block = block

	if s.blocks == nil {
		s.blocks = make(map[common.Hash]*types.Block)
	}

	if _, ok := s.blocks[hash]; ok {
		for i, existing := range s.order {
			if existing == hash {
				copy(s.order[i:], s.order[i+1:])
				s.order = s.order[:len(s.order)-1]
				break
			}
		}
	}

	s.order = append(s.order, hash)
	s.blocks[hash] = block

	for len(s.order) > recentBlockBuiltCapacity {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.blocks, oldest)
	}
}

func (s *LatestBlockBuiltStore) BlockBuilt() *types.Block {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.block
}

func (s *LatestBlockBuiltStore) BlockBuiltByHash(hash common.Hash) *types.Block {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.blocks[hash]
}
