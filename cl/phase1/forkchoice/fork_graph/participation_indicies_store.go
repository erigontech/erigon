package fork_graph

import (
	"bytes"
	"sync"
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
		p.s.Store(epoch, bytes.Clone(participations))
		return
	}
	// Reuse the existing slice if possible
	prevBitlist := prevBitlistInterface.([]byte)
	prevBitlist = prevBitlist[:0]
	p.s.Store(epoch, append(prevBitlist, participations...))
}

func (p *participationIndiciesStore) keysThrough(epoch uint64) []uint64 {
	keys := []uint64{}
	p.s.Range(func(key, _ any) bool {
		if key.(uint64) <= epoch {
			keys = append(keys, key.(uint64))
		}
		return true
	})
	return keys
}

func (p *participationIndiciesStore) deleteKeys(keys []uint64) {
	for _, key := range keys {
		p.s.Delete(key)
	}
}
