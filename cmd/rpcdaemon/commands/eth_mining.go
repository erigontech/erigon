package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// Propose changing this file name to eth_mining.go and adding missing mining related commands

// Coinbase is the address that mining rewards will be sent to
func (api *APIImpl) Coinbase(_ context.Context) (common.Address, error) {
	var stub common.Address
	return stub, fmt.Errorf(NotImplemented, "eth_coinbase")
	// if api.ethBackend == nil {
	// 	// We're running in --chaindata mode or otherwise cannot get the backend
	// 	return common.Address{}, fmt.Errorf(NotAvailableChainData, "eth_coinbase")
	// }
	// return api.ethBackend.Etherbase()
}

// Hashrate returns the number of hashes per second that the node is mining with.
func (api *APIImpl) Hashrate(_ context.Context) (uint64, error) {
	return 0, fmt.Errorf(NotImplemented, "eth_hashRate")
}

// Mining returns true if client is actively mining new blocks.
func (api *APIImpl) Mining(_ context.Context) (bool, error) {
	return false, fmt.Errorf(NotImplemented, "eth_mining")
}

// GetWork returns the hash of the current block, the seedHash, and the boundary condition to be met.
// Returns Array - Array with the following properties:
//		DATA, 32 Bytes - current block header pow-hash
//		DATA, 32 Bytes - the seed hash used for the DAG.
//		DATA, 32 Bytes - the boundary condition ("target"), 2^256 / difficulty.
func (api *APIImpl) GetWork(_ context.Context) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "eth_getWork")
}

// SubmitWork used for submitting a proof-of-work solution.
func (api *APIImpl) SubmitWork(_ context.Context, nonce rpc.BlockNumber, powHash, digest common.Hash) (bool, error) {
	return false, fmt.Errorf(NotImplemented, "eth_submitWork")
}

// SubmitHashrate used for submitting mining hashrate.
func (api *APIImpl) SubmitHashrate(_ context.Context, hashRate common.Hash, id string) (bool, error) {
	return false, fmt.Errorf(NotImplemented, "eth_sumitHashrate")
}
