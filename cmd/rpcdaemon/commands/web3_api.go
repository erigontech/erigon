package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

// Web3API provides interfaces for the web3_ RPC commands
type Web3API interface {
	ClientVersion(_ context.Context) (string, error)
	Sha3(_ context.Context, input hexutil.Bytes) hexutil.Bytes
}

type Web3APIImpl struct {
	*BaseAPI
	api core.ApiBackend
}

// NewWeb3APIImpl returns Web3APIImpl instance
func NewWeb3APIImpl(api core.ApiBackend) *Web3APIImpl {
	return &Web3APIImpl{
		BaseAPI: &BaseAPI{},
		api:     api,
	}
}

// ClientVersion implements web3_clientVersion. Returns the current client version.
func (api *Web3APIImpl) ClientVersion(ctx context.Context) (string, error) {
	return api.ClientVersion(ctx)
}

// Sha3 implements web3_sha3. Returns Keccak-256 (not the standardized SHA3-256) of the given data.
func (api *Web3APIImpl) Sha3(_ context.Context, input hexutil.Bytes) hexutil.Bytes {
	return crypto.Keccak256(input)
}
