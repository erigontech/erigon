package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	txPoolProto "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

// SendRawTransaction implements eth_sendRawTransaction. Creates new message call transaction or a contract creation for previously-signed transactions.
func (api *APIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	txn, err := types.DecodeWrappedTransaction(encodedTx)
	if err != nil {
		return common.Hash{}, err
	}

	if txn.Type() == types.BlobTxType || txn.Type() == types.DynamicFeeTxType || txn.Type() == types.SetCodeTxType {
		blockGasLimit := api.BlockGasLimit(ctx)

		// If the transaction fee cap is already specified, ensure the
		// effective gas fee is less than fee cap.
		if err := checkDynamicTxFee(txn.GetFeeCap(), blockGasLimit); err != nil {
			return common.Hash{}, err
		}
	} else {
		// If the transaction fee cap is already specified, ensure the
		// fee of the given transaction is _reasonable_.
		if err := checkTxFee(txn.GetPrice().ToBig(), txn.GetGas(), api.FeeCap); err != nil {
			return common.Hash{}, err
		}
	}

	if !txn.Protected() && !api.AllowUnprotectedTxs {
		return common.Hash{}, errors.New("only replay-protected (EIP-155) transactions allowed over RPC")
	}

	// this has been moved to prior to adding of transactions to capture the
	// pre state of the db - which is used for logging in the messages below
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return common.Hash{}, err
	}

	defer tx.Rollback()

	cc, err := api.chainConfig(ctx, tx)
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

// checkDynamicTxFee checks if the provided gas cap exceeds the block gas limit.
// It returns an error if the gas cap is greater than the block gas limit.
// checkDynamicTxFee checks if the provided gas cap is within acceptable limits
// compared to the block gas limit. It calculates a gas limit as 1.5 times the
// block gas limit and compares it to the gas cap.
func checkDynamicTxFee(gasCap *uint256.Int, blockGasLimit uint64) error {
	gasLimit := uint256.NewInt(0)
	gasLimit.SetUint64(blockGasLimit + (blockGasLimit / 2))

	if gasLimit.Lt(gasCap) {
		return errors.New("fee cap is bigger than the block gas limit")
	}

	return nil
}
