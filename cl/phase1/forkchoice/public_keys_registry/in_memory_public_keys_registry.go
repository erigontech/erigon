package public_keys_registry

import (
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils/bls"
)

type InMemoryPublicKeysRegistry struct {
	basePublicKeyRegistry []common.Bytes48
	statesRegistry        map[solid.Checkpoint][]common.Bytes48

	mu sync.RWMutex
}

func NewInMemoryPublicKeysRegistry() *InMemoryPublicKeysRegistry {
	return &InMemoryPublicKeysRegistry{
		basePublicKeyRegistry: make([]common.Bytes48, 0),
		statesRegistry:        make(map[solid.Checkpoint][]common.Bytes48),
	}
}

// // PublicKeyRegistry is a registry of public keys
// // It is used to store public keys and their indices
// type PublicKeyRegistry interface {
// 	ResetAnchor(s abstract.BeaconState)
// 	VerifyAggregateSignature(blockRoot common.Hash, slot uint64, pubkeysIdxs []uint64, message []byte, signature common.Bytes96) error
//  AddState(epoch uint64, blockRoot common.Hash, s abstract.BeaconState)
// 	Prune(epoch uint64)
// }

// ResetAnchor resets the public keys registry to the anchor state
func (r *InMemoryPublicKeysRegistry) ResetAnchor(s abstract.BeaconState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statesRegistry = make(map[solid.Checkpoint][]common.Bytes48)        // reset the registry
	r.basePublicKeyRegistry = make([]common.Bytes48, s.ValidatorLength()) // reset the public keys registry
	for i := 0; i < s.ValidatorLength(); i++ {
		copy(r.basePublicKeyRegistry[i][:], s.ValidatorSet().Get(i).PublicKeyBytes())
	}
}

// VerifyAggregateSignature verifies the aggregate signature
func (r *InMemoryPublicKeysRegistry) VerifyAggregateSignature(checkpoint solid.Checkpoint, pubkeysIdxs *solid.RawUint64List, message []byte, signature common.Bytes96) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if pubkeysIdxs.Length() == 0 {
		return false, fmt.Errorf("no public keys provided, %d, %s", checkpoint.Epoch, checkpoint.Root)
	}
	basePublicKeyRegistryLength := uint64(len(r.basePublicKeyRegistry))
	statePublicKeys, ok := r.statesRegistry[checkpoint]
	if !ok {
		return false, fmt.Errorf("public keys registry not found for epoch %d and block root %s", checkpoint.Epoch, checkpoint.Root)
	}

	pks := make([][]byte, 0, pubkeysIdxs.Length())
	pubkeysIdxs.Range(func(_ int, value uint64, length int) bool {
		if value >= basePublicKeyRegistryLength {
			if value-basePublicKeyRegistryLength >= uint64(len(statePublicKeys)) {
				return false
			}
			pks = append(pks, statePublicKeys[value-basePublicKeyRegistryLength][:])
			return true
		}
		pks = append(pks, r.basePublicKeyRegistry[value][:])
		return true
	})
	return bls.VerifyAggregate(signature[:], message, pks)
}

// AddState adds the state to the public keys registry
func (r *InMemoryPublicKeysRegistry) AddState(checkpoint solid.Checkpoint, s abstract.BeaconState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	statePublicKeys := make([]common.Bytes48, s.ValidatorLength()-len(r.basePublicKeyRegistry))
	for i := len(r.basePublicKeyRegistry); i < s.ValidatorLength(); i++ {
		copy(statePublicKeys[i-len(r.basePublicKeyRegistry)][:], s.ValidatorSet().Get(i).PublicKeyBytes())
	}
	r.statesRegistry[checkpoint] = statePublicKeys
}

// Prune removes the public keys registry for the given epoch
func (r *InMemoryPublicKeysRegistry) Prune(epoch uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.statesRegistry {
		if k.Epoch < epoch {
			delete(r.statesRegistry, k)
		}
	}
}
