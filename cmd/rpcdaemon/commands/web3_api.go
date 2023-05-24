package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

// Web3API provides interfaces for the web3_ RPC commands
type Web3API interface {
	ClientVersion(_ context.Context) (string, error)
	Sha3(_ context.Context, input hexutility.Bytes) hexutility.Bytes
}

type Web3APIImpl struct {
	*BaseAPI
	ethBackend rpchelper.ApiBackend
}

// NewWeb3APIImpl returns Web3APIImpl instance
func NewWeb3APIImpl(ethBackend rpchelper.ApiBackend) *Web3APIImpl {
	return &Web3APIImpl{
		BaseAPI:    &BaseAPI{},
		ethBackend: ethBackend,
	}
}

// ClientVersion implements web3_clientVersion. Returns the current client version.
func (api *Web3APIImpl) ClientVersion(ctx context.Context) (string, error) {
	return api.ethBackend.ClientVersion(ctx)
}

// Sha3 implements web3_sha3. Returns Keccak-256 (not the standardized SHA3-256) of the given data.
func (api *Web3APIImpl) Sha3(_ context.Context, input hexutility.Bytes) hexutility.Bytes {
	return crypto.Keccak256(input)
}
