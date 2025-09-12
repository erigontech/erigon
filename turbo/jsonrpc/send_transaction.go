package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"github.com/erigontech/erigon/zk/hermez_db"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	txPoolProto "github.com/erigontech/erigon-lib/gointerfaces/txpool"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/zk/utils"
)

// SendRawTransaction implements eth_sendRawTransaction. Creates new message call transaction or a contract creation for previously-signed transactions.
func (api *APIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutility.Bytes) (common.Hash, error) {
	t := utils.StartTimer("rpc", "sendrawtransaction")
	defer t.LogTimer()

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	defer tx.Rollback()
	cc, err := api.chainConfig(ctx, tx)
	if err != nil {
		return common.Hash{}, err
	}
	chainId := cc.ChainID

	// [zkevm] - proxy the request if the chainID is ZK and not a sequencer
	if api.isZkNonSequencer(chainId) {
		// [zkevm] - proxy the request to the pool manager if the pool manager is set
		if api.isPoolManagerAddressSet() {
			return api.sendTxZk(api.PoolManagerUrl, encodedTx, chainId.Uint64())
		}

		return api.sendTxZk(api.l2RpcUrl, encodedTx, chainId.Uint64())
	}

	txn, err := types.DecodeWrappedTransaction(encodedTx)
	if err != nil {
		return common.Hash{}, err
	}

	latestBlockNumber, err := rpchelper.GetLatestFinishedBlockNumber(tx)
	if err != nil {
		return common.Hash{}, err
	}

	header, err := api.loadSendTransactionBlock(ctx, tx, latestBlockNumber)
	if err != nil {
		return common.Hash{}, err
	}

	// now get the sender and put a lock in place for them
	signer := types.MakeSigner(cc, latestBlockNumber, header.Time())
	sender, err := txn.Sender(*signer)
	if err != nil {
		return common.Hash{}, err
	}
	api.SenderLocks.AddLock(sender)
	defer api.SenderLocks.ReleaseLock(sender)

	// When we are not in normalcy, we need to check if the transaction is a legacy transaction
	if !cc.IsNormalcy(header.NumberU64()) && txn.Type() != types.LegacyTxType {
		return common.Hash{}, errors.New("only legacy transactions are supported")
	}

	// We do not support blob transactions on L2
	if txn.Type() == types.BlobTxType {
		return common.Hash{}, errors.New("blob transactions are not supported")
	}

	// check if the price is too low if we are set to reject low gas price transactions
	if api.RejectLowGasPriceTransactions &&
		ShouldRejectLowGasPrice(
			txn.GetFeeCap().ToBig(),
			api.getLowestPrice(ctx, cc, header),
			api.RejectLowGasPriceTolerance,
		) {
		return common.Hash{}, errors.New("transaction price is too low")
	}

	// If the transaction fee cap is already specified, ensure the
	// fee of the given transaction is _reasonable_.
	if err = checkTxFee(txn.GetFeeCap().ToBig(), txn.GetGas(), api.FeeCap); err != nil {
		return common.Hash{}, err
	}

	if !api.AllowPreEIP155Transactions && !txn.Protected() && !api.AllowUnprotectedTxs {
		return common.Hash{}, errors.New("only replay-protected (EIP-155) transactions allowed over RPC")
	}

	if txn.Protected() {
		txnChainId := txn.GetChainID()
		if chainId.Cmp(txnChainId.ToBig()) != 0 {
			return common.Hash{}, fmt.Errorf("invalid chain id, expected: %d got: %d", chainId, *txnChainId)
		}
	}

	hash := txn.Hash()

	// [zkevm] - check if the transaction is a bad one
	hermezDb := hermez_db.NewHermezDbReader(tx)
	badTxHashCounter, err := hermezDb.GetBadTxHashCounter(hash)
	if err != nil {
		return common.Hash{}, err
	}

	if badTxHashCounter >= api.BadTxAllowance {
		return common.Hash{}, errors.New("transaction uses too many counters to fit into a batch")
	}

	res, err := api.txPool.Add(ctx, &txPoolProto.AddRequest{RlpTxs: [][]byte{encodedTx}})
	if err != nil {
		return common.Hash{}, err
	}

	if res.Imported[0] != txPoolProto.ImportResult_SUCCESS {
		return hash, fmt.Errorf("%s: %s", txPoolProto.ImportResult_name[int32(res.Imported[0])], res.Errors[0])
	}

	return txn.Hash(), nil
}

// SendTransaction implements eth_sendTransaction. Creates new message call transaction or a contract creation if the data field contains code.
func (api *APIImpl) SendTransaction(_ context.Context, txObject interface{}) (common.Hash, error) {
	return common.Hash{0}, fmt.Errorf(NotImplemented, "eth_sendTransaction")
}

func (api *APIImpl) loadSendTransactionBlock(ctx context.Context, tx kv.Tx, blockNumber uint64) (*types.Block, error) {
	if api.sendTransactionBlockCache == nil {
		// must have been some error during initialisation - so just load the block as normal
		return api.blockByNumber(ctx, rpc.BlockNumber(blockNumber), tx)
	}

	// Check cache first (fast path for cache hits)
	if block, ok := api.sendTransactionBlockCache.Get(blockNumber); ok {
		return block, nil
	}

	// Use singleflight to ensure only one goroutine loads each block number
	key := strconv.FormatUint(blockNumber, 10)
	result, err, _ := api.sendTransactionBlockGroup.Do(key, func() (interface{}, error) {
		if block, ok := api.sendTransactionBlockCache.Get(blockNumber); ok {
			return block, nil
		}

		// Load the block
		block, err := api.blockByNumber(ctx, rpc.BlockNumber(blockNumber), tx)
		if err != nil {
			return nil, err
		}

		// Cache the result
		api.sendTransactionBlockCache.Add(blockNumber, block)
		return block, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*types.Block), nil
}

// checkTxFee is an internal function used to check whether the fee of
// the given transaction is _reasonable_(under the cap).
func checkTxFee(gasPrice *big.Int, gas uint64, gasCap float64) error {
	// Short circuit if there is no gasCap for transaction fee at all.
	if gasCap == 0 {
		return nil
	}
	feeEth := new(big.Float).Quo(new(big.Float).SetInt(new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gas))), new(big.Float).SetInt(big.NewInt(params.Ether)))
	feeFloat, _ := feeEth.Float64()
	if feeFloat > gasCap {
		return fmt.Errorf("tx fee (%.2f ether) exceeds the configured cap (%.2f ether)", feeFloat, gasCap)
	}
	return nil
}

func ShouldRejectLowGasPrice(txPrice *big.Int, lowestAllowed *big.Int, rejectLowGasPriceTolerance float64) bool {
	finalCheck := new(big.Int).Set(lowestAllowed)
	if rejectLowGasPriceTolerance > 0 {
		modifier := new(big.Int).SetUint64(uint64(100 - rejectLowGasPriceTolerance*100))
		finalCheck.Mul(finalCheck, modifier)
		finalCheck.Div(finalCheck, big.NewInt(100))
	}
	return txPrice.Cmp(finalCheck) < 0
}

func (api *APIImpl) getLowestPrice(ctx context.Context, chainConfig *chain.Config, block *types.Block) *big.Int {
	lowestPrice := api.gasTracker.GetLowestPrice()
	if chainConfig.IsNormalcy(block.NumberU64()) && chainConfig.IsLondon(block.NumberU64()) {
		if block.BaseFee() != nil {
			lowestPrice.Add(lowestPrice, block.BaseFee())
		}
	}
	return lowestPrice
}
