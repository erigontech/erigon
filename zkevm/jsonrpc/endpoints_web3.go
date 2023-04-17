package jsonrpc

import (
	"math/big"

	"github.com/0xPolygonHermez/zkevm-node"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
	"golang.org/x/crypto/sha3"
)

// Web3Endpoints contains implementations for the "web3" RPC endpoints
type Web3Endpoints struct {
}

// ClientVersion returns the client version.
func (e *Web3Endpoints) ClientVersion() (interface{}, types.Error) {
	return zkevm.Version, nil
}

// Sha3 returns the keccak256 hash of the given data.
func (e *Web3Endpoints) Sha3(data types.ArgBig) (interface{}, types.Error) {
	b := (*big.Int)(&data)
	hash := sha3.NewLegacyKeccak256()
	hash.Write(b.Bytes()) //nolint:errcheck,gosec
	keccak256Hash := hash.Sum(nil)
	return types.ArgBytes(keccak256Hash), nil
}
