package public_keys_registry

import (
	"context"

	"github.com/Giulio2002/bls"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
)

type DBPublicKeysRegistry struct {
	db kv.RoDB
}

func NewDBPublicKeysRegistry(db kv.RoDB) *DBPublicKeysRegistry {
	return &DBPublicKeysRegistry{db: db}
}

// ResetAnchor resets the public keys registry to the anchor state
func (r *DBPublicKeysRegistry) ResetAnchor(s abstract.BeaconState) {
	// no-op
}

// VerifyAggregateSignature verifies the aggregate signature
func (r *DBPublicKeysRegistry) VerifyAggregateSignature(checkpoint solid.Checkpoint, pubkeysIdxs *solid.RawUint64List, message []byte, signature common.Bytes96) (bool, error) {
	pks := make([][]byte, 0, pubkeysIdxs.Length())

	if err := r.db.View(context.TODO(), func(tx kv.Tx) error {
		pubkeysIdxs.Range(func(_ int, value uint64, length int) bool {
			pk, err := state_accessors.ReadPublicKeyByIndexNoCopy(tx, value)
			if err != nil {
				return false
			}
			pks = append(pks, pk)
			return true
		})
		return nil
	}); err != nil {
		return false, err
	}

	return bls.VerifyAggregate(signature[:], message, pks)
}

// AddState adds the state to the public keys registry
func (r *DBPublicKeysRegistry) AddState(checkpoint solid.Checkpoint, s abstract.BeaconState) {
	// no-op
}

// Prune removes the public keys registry for the given epoch
func (r *DBPublicKeysRegistry) Prune(epoch uint64) {
	// no-op
}
