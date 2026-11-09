package accounts

import (
	"encoding/binary"
	"os"
	"sync"
	"unique"
	"unsafe"

	"github.com/erigontech/erigon/common"
)

var internNoWeak = os.Getenv("INTERN_NOWEAK") == "1"

const noWeakShardCount = 4 * 1024

type noWeakShard[T comparable] struct {
	sync.RWMutex
	values map[T]*T
}

var (
	noWeakAddresses [noWeakShardCount]noWeakShard[common.Address]
	noWeakHashes    [noWeakShardCount]noWeakShard[common.Hash]
)

func (s *noWeakShard[T]) load(v T) *T {
	s.RLock()
	defer s.RUnlock()
	return s.values[v]
}

func (s *noWeakShard[T]) store(v T) *T {
	s.Lock()
	defer s.Unlock()
	if p := s.values[v]; p != nil {
		return p
	}
	if s.values == nil {
		s.values = make(map[T]*T)
	}
	p := new(v)
	s.values[v] = p
	return p
}

// handle relies on unique.Handle[T] being a struct with a single *T field.
func (s *noWeakShard[T]) handle(v T) unique.Handle[T] {
	p := s.load(v)
	if p == nil {
		p = s.store(v)
	}
	return *(*unique.Handle[T])(unsafe.Pointer(&p))
}

func makeAddress(a common.Address) Address {
	if internNoWeak {
		return Address(noWeakAddresses[binary.BigEndian.Uint32(a[16:])&(noWeakShardCount-1)].handle(a))
	}
	return Address(unique.Make(a))
}

func makeHash(k common.Hash) unique.Handle[common.Hash] {
	if internNoWeak {
		return noWeakHashes[(binary.BigEndian.Uint32(k[:4])^binary.BigEndian.Uint32(k[28:]))&(noWeakShardCount-1)].handle(k)
	}
	return unique.Make(k)
}
