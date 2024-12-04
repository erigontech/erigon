package fork_graph

import (
	"sync"

	"github.com/erigontech/erigon-lib/common"
)

type participationIndiciesStore struct {
	s sync.Map
}

func (p *participationIndiciesStore) get(epoch uint64) ([]byte, bool) {
	val, ok := p.s.Load(epoch)
	if !ok {
		return nil, false
	}
	return val.([]byte), true
}

func (p *participationIndiciesStore) add(epoch uint64, participations []byte) {
	prevBitlistInterface, ok := p.s.Load(epoch)
	if !ok {
		p.s.Store(epoch, common.Copy(participations))
		return
	}
	// Reuse the existing slice if possible
	prevBitlist := prevBitlistInterface.([]byte)
	prevBitlist = prevBitlist[:0]
	p.s.Store(epoch, append(prevBitlist, participations...))
}

func (p *participationIndiciesStore) prune(epoch uint64) {
	// iterate over the map and delete all keys less or equal than epoch
	p.s.Range(func(key, value interface{}) bool {
		if key.(uint64) <= epoch {
			p.s.Delete(key)
		}
		return true
	})
}
