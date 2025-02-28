package public_keys_registry

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils/bls"
)

type DBPublicKeysRegistry struct {
	headView synced_data.SyncedData
}

func NewHeadViewPublicKeysRegistry(headView synced_data.SyncedData) *DBPublicKeysRegistry {
	return &DBPublicKeysRegistry{headView: headView}
}

// ResetAnchor resets the public keys registry to the anchor state
func (r *DBPublicKeysRegistry) ResetAnchor(s abstract.BeaconState) {
	// no-op
}

// VerifyAggregateSignature verifies the aggregate signature
func (r *DBPublicKeysRegistry) VerifyAggregateSignature(checkpoint solid.Checkpoint, pubkeysIdxs *solid.RawUint64List, message []byte, signature common.Bytes96) (bool, error) {
	pks := make([][]byte, 0, pubkeysIdxs.Length())

	pubkeysIdxs.Range(func(_ int, value uint64, length int) bool {
		pk, err := r.headView.ValidatorPublicKeyByIndex(int(value))
		if err != nil {
			return false
		}
		pks = append(pks, pk[:])
		return true
	})

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
