package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// Coinbase implements eth_coinbase. Returns the current client coinbase address.
func (api *APIImpl) Coinbase(_ context.Context) (common.Address, error) {
	var stub common.Address
	return stub, fmt.Errorf(NotImplemented, "eth_coinbase")
	// if api.ethBackend == nil {
	// 	// We're running in --chaindata mode or otherwise cannot get the backend
	// 	return common.Address{}, fmt.Errorf(NotAvailableChainData, "eth_coinbase")
	// }
	// return api.ethBackend.Etherbase()
}

// Hashrate implements eth_hashrate. Returns the number of hashes per second that the node is mining with.
func (api *APIImpl) Hashrate(_ context.Context) (uint64, error) {
	return 0, fmt.Errorf(NotImplemented, "eth_hashRate")
}

// Mining implements eth_mining. Returns true if client is actively mining new blocks.
func (api *APIImpl) Mining(_ context.Context) (bool, error) {
	return false, fmt.Errorf(NotImplemented, "eth_mining")
}

// GetWork implements eth_getWork. Returns the hash of the current block, the seedHash, and the boundary condition to be met ('target').
func (api *APIImpl) GetWork(_ context.Context) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "eth_getWork")
}

// SubmitWork implements eth_submitWork. Submits a proof-of-work solution to the blockchain.
func (api *APIImpl) SubmitWork(_ context.Context, nonce rpc.BlockNumber, powHash, digest common.Hash) (bool, error) {
	return false, fmt.Errorf(NotImplemented, "eth_submitWork")
}

// SubmitHashrate implements eth_submitHashrate. Submit the mining hashrate to the blockchain.
func (api *APIImpl) SubmitHashrate(_ context.Context, hashRate common.Hash, id string) (bool, error) {
	return false, fmt.Errorf(NotImplemented, "eth_sumitHashrate")
}
