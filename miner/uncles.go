package miner

import (
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type miningUncles struct {
	localUncles  map[common.Hash]*types.Block // A set of side blocks generated locally as the possible uncle blocks.
	remoteUncles map[common.Hash]*types.Block // A set of side blocks as the possible uncle blocks.
	sync.RWMutex
}

func newUncles() *miningUncles {
	return &miningUncles{
		localUncles:  make(map[common.Hash]*types.Block),
		remoteUncles: make(map[common.Hash]*types.Block),
	}
}

func (u *miningUncles) getLocal(hash common.Hash) (*types.Block, bool) {
	u.RLock()
	defer u.RUnlock()
	b, ok := u.localUncles[hash]
	return b, ok
}

func (u *miningUncles) setLocal(b *types.Block) {
	u.Lock()
	defer u.Unlock()
	u.localUncles[b.Hash()] = b
}

func (u *miningUncles) getRemote(hash common.Hash) (*types.Block, bool) {
	u.RLock()
	defer u.RUnlock()
	b, ok := u.remoteUncles[hash]
	return b, ok
}

func (u *miningUncles) setRemote(b *types.Block) {
	u.Lock()
	defer u.Unlock()
	u.remoteUncles[b.Hash()] = b
}

func (u *miningUncles) get(hash common.Hash) (*types.Block, bool) {
	u.RLock()
	defer u.RUnlock()

	uncle, exist := u.getLocal(hash)
	if !exist {
		uncle, exist = u.getRemote(hash)
	}
	if !exist {
		return nil, false
	}

	return uncle, true
}

func (u *miningUncles) has(hash common.Hash) bool {
	u.RLock()
	defer u.RUnlock()
	_, ok := u.localUncles[hash]
	if ok {
		return true
	}

	_, ok = u.remoteUncles[hash]
	if ok {
		return ok
	}
	return ok
}
