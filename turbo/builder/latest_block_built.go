package builder

import (
	"sync"

	"github.com/ledgerwatch/erigon/core/types"
)

type LatestBlockBuiltStore struct {
	block *types.Block

	lock sync.Mutex
}

func NewLatestBlockBuiltStore() *LatestBlockBuiltStore {
	return &LatestBlockBuiltStore{}
}

func (s *LatestBlockBuiltStore) AddBlockBuilt(block *types.Block) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.block = block
}

func (s *LatestBlockBuiltStore) BlockBuilt() *types.Block {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.block
}
