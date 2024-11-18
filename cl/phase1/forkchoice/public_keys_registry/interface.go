package public_keys_registry

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// This package as a whole gets plenty of test coverage in spectests, so we can skip unit testing here.

// PublicKeyRegistry is a registry of public keys
// It is used to store public keys and their indices
type PublicKeyRegistry interface {
	ResetAnchor(s abstract.BeaconState)
	VerifyAggregateSignature(checkpoint solid.Checkpoint, pubkeysIdxs *solid.RawUint64List, message []byte, signature common.Bytes96) (bool, error)
	AddState(checkpoint solid.Checkpoint, s abstract.BeaconState)
	Prune(epoch uint64)
}
