package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	txPoolProto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/zk/utils"
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

	if txn.Type() != types.LegacyTxType {
		latestBlock, err := api.blockByNumber(ctx, rpc.LatestBlockNumber, tx)

		if err != nil {
			return common.Hash{}, err
		}

		if !cc.IsLondon(latestBlock.NumberU64()) {
			return common.Hash{}, errors.New("only legacy transactions are supported")
		}

		if txn.Type() == types.BlobTxType {
			return common.Hash{}, errors.New("blob transactions are not supported")
		}
	}

	// If the transaction fee cap is already specified, ensure the
	// fee of the given transaction is _reasonable_.
	if err := checkTxFee(txn.GetPrice().ToBig(), txn.GetGas(), api.FeeCap); err != nil {
		return common.Hash{}, err
	}
	if !api.AllowPreEIP155Transactions && !txn.Protected() && !api.AllowUnprotectedTxs {
		return common.Hash{}, errors.New("only replay-protected (EIP-155) transactions allowed over RPC")
	}

	// this has been moved to prior to adding of transactions to capture the
	// pre state of the db - which is used for logging in the messages below
	tx, err = api.db.BeginRo(ctx)
	if err != nil {
		return common.Hash{}, err
	}

	defer tx.Rollback()

	cc, err = api.chainConfig(ctx, tx)
	if err != nil {
		return common.Hash{}, err
	}

	if txn.Protected() {
		txnChainId := txn.GetChainID()
		chainId := cc.ChainID
		if chainId.Cmp(txnChainId.ToBig()) != 0 {
			return common.Hash{}, fmt.Errorf("invalid chain id, expected: %d got: %d", chainId, *txnChainId)
		}
	}

	hash := txn.Hash()
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
