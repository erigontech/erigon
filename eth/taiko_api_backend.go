package eth

import (
	"context"
	"math/big"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/erigon-db/rawdb"
)

// TaikoAPIBackend handles L2 node related RPC calls.
type TaikoAPIBackend struct {
	eth *Ethereum
}

// NewTaikoAPIBackend creates a new TaikoAPIBackend instance.
func NewTaikoAPIBackend(eth *Ethereum) *TaikoAPIBackend {
	return &TaikoAPIBackend{
		eth: eth,
	}
}

// HeadL1Origin returns the latest L2 block's corresponding L1 origin.
func (s *TaikoAPIBackend) HeadL1Origin() (*rawdb.L1Origin, error) {
	tx, err := s.eth.ChainDB().BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockID, err := rawdb.ReadHeadL1Origin(tx)
	if err != nil {
		return nil, err
	}

	if blockID == nil {
		return nil, ethereum.NotFound
	}

	l1Origin, err := rawdb.ReadL1Origin(tx, blockID)
	if err != nil {
		return nil, err
	}

	if l1Origin == nil {
		return nil, ethereum.NotFound
	}

	return l1Origin, nil
}

// L1OriginByID returns the L2 block's corresponding L1 origin.
func (s *TaikoAPIBackend) L1OriginByID(blockID *math.HexOrDecimal256) (*rawdb.L1Origin, error) {
	tx, err := s.eth.ChainDB().BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	l1Origin, err := rawdb.ReadL1Origin(tx, (*big.Int)(blockID))
	if err != nil {
		return nil, err
	}

	if l1Origin == nil {
		return nil, ethereum.NotFound
	}

	return l1Origin, nil
}

// GetSyncMode returns the node sync mode.
func (s *TaikoAPIBackend) GetSyncMode() (string, error) {
	return "full", nil //TODO change hardcoded value
}

// TaikoAuthAPIBackend handles L2 node related authorized RPC calls.
type TaikoAuthAPIBackend struct {
	eth *Ethereum
}

// NewTaikoAuthAPIBackend creates a new TaikoAuthAPIBackend instance.
func NewTaikoAuthAPIBackend(eth *Ethereum) *TaikoAuthAPIBackend {
	return &TaikoAuthAPIBackend{eth}
}

// SetHeadL1Origin sets the latest L2 block's corresponding L1 origin.
func (a *TaikoAuthAPIBackend) SetHeadL1Origin(blockID *math.HexOrDecimal256) (*big.Int, error) {
	tx, err := a.eth.ChainDB().BeginRw(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	err = rawdb.WriteHeadL1Origin(tx, (*big.Int)(blockID))
	return (*big.Int)(blockID), err
}

// UpdateL1Origin updates the L2 block's corresponding L1 origin.
func (a *TaikoAuthAPIBackend) UpdateL1Origin(l1Origin *rawdb.L1Origin) (*rawdb.L1Origin, error) {
	tx, err := a.eth.ChainDB().BeginRw(context.Background())
	if err != nil {
		return nil, err
	}
	err = rawdb.WriteL1Origin(tx, l1Origin.BlockID, l1Origin)
	return l1Origin, err
}

// TODO(taiko) : Implement this using Erigon txpool
// // TxPoolContent retrieves the transaction pool content with the given upper limits.
// func (a *TaikoAuthAPIBackend) TxPoolContent(
// 	beneficiary common.Address,
// 	baseFee *big.Int,
// 	blockMaxGasLimit uint64,
// 	maxBytesPerTxList uint64,
// 	locals []string,
// 	maxTransactionsLists uint64,
// ) ([]*miner.PreBuiltTxList, error) {
// 	log.Debug(
// 		"Fetching L2 pending transactions finished",
// 		"baseFee", baseFee,
// 		"blockMaxGasLimit", blockMaxGasLimit,
// 		"maxBytesPerTxList", maxBytesPerTxList,
// 		"maxTransactions", maxTransactionsLists,
// 		"locals", locals,
// 	)
// 	a.eth.miningRpcClient

// 	return a.eth.Miner().BuildTransactionsLists(
// 		beneficiary,
// 		baseFee,
// 		blockMaxGasLimit,
// 		maxBytesPerTxList,
// 		locals,
// 		maxTransactionsLists,
// 	)
// }

// TODO(taiko): implement this using Erigon txpool
// // TxPoolContentWithMinTip retrieves the transaction pool content with the given upper limits and minimum tip.
// func (a *TaikoAuthAPIBackend) TxPoolContentWithMinTip(
// 	beneficiary common.Address,
// 	baseFee *big.Int,
// 	blockMaxGasLimit uint64,
// 	maxBytesPerTxList uint64,
// 	locals []string,
// 	maxTransactionsLists uint64,
// 	minTip uint64,
// ) ([]*miner.PreBuiltTxList, error) {
// 	a.eth.logger.Debug(
// 		"Fetching L2 pending transactions finished",
// 		"baseFee", baseFee,
// 		"blockMaxGasLimit", blockMaxGasLimit,
// 		"maxBytesPerTxList", maxBytesPerTxList,
// 		"maxTransactions", maxTransactionsLists,
// 		"locals", locals,
// 		"minTip", minTip,
// 	)

// 	return a.eth.Miner().BuildTransactionsListsWithMinTip(
// 		beneficiary,
// 		baseFee,
// 		blockMaxGasLimit,
// 		maxBytesPerTxList,
// 		locals,
// 		maxTransactionsLists,
// 		minTip,
// 	)
// }
