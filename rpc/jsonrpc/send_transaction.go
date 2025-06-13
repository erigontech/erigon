package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	txPoolProto "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/types"
)

// SendRawTransaction implements eth_sendRawTransaction. Creates new message call transaction or a contract creation for previously-signed transactions.
func (api *APIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	txn, err := types.DecodeWrappedTransaction(encodedTx)
	if err != nil {
		return common.Hash{}, err
	}

	// TODO arbitrum - this code is still needed?
	if txn.Type() == types.BlobTxType || txn.Type() == types.DynamicFeeTxType || txn.Type() == types.SetCodeTxType {
		baseFeeBig, err := api.BaseFee(ctx)
		if err != nil {
			return common.Hash{}, err
		}

		// If the transaction fee cap is already specified, ensure the
		// effective gas fee is less than fee cap.
		if err := checkDynamicTxFee(txn.GetFeeCap(), baseFeeBig); err != nil {
			return common.Hash{}, err
		}
	} else {
		// If the transaction fee cap is already specified, ensure the
		// fee of the given transaction is _reasonable_.
		if err := CheckTxFee(txn.GetFeeCap().ToBig(), txn.GetGasLimit(), api.FeeCap); err != nil {
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

// CheckTxFee is an internal function used to check whether the fee of
// the given transaction is _reasonable_(under the cap).
func CheckTxFee(gasPrice *big.Int, gas uint64, gasCap float64) error {
	// Short circuit if there is no gasCap for transaction fee at all.
	if gasCap == 0 {
		return nil
	}

	feeEth := new(big.Float).Quo(new(big.Float).SetInt(new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gas))), new(big.Float).SetInt(big.NewInt(common.Ether)))
	feeFloat, _ := feeEth.Float64()
	if feeFloat > gasCap {
		return fmt.Errorf("tx fee (%.2f ether) exceeds the configured cap (%.2f ether)", feeFloat, gasCap)
	}

	return nil
}

// TODO arbitrum - needed ?
// checkTxFee is an internal function used to check whether the fee of
// the given transaction is _reasonable_(under the cap).
func checkDynamicTxFee(gasCap *uint256.Int, baseFeeBig *hexutil.Big) error {
	baseFee := uint256.NewInt(0)
	overflow := baseFee.SetFromBig(baseFeeBig.ToInt())
	if overflow {
		return errors.New("opts.Value higher than 2^256-1")
	}

	if gasCap.Lt(baseFee) {
		return errors.New("fee cap is lower than the base fee")
	}

	return nil
}
