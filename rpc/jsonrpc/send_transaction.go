package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// SendRawTransaction implements eth_sendRawTransaction. Creates a new message call or contract creation for a previously signed transaction.
func (api *APIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	if err := txpool.ValidateSerializedTxn(encodedTx); err != nil {
		return common.Hash{}, err
	}
	txn, err := types.DecodeWrappedTransaction(encodedTx)
	if err != nil {
		return common.Hash{}, err
	}

	// If the transaction fee cap is already specified, ensure the
	// fee of the given transaction is _reasonable_.
	if err := checkTxFee(txn.GetFeeCap().ToBig(), txn.GetGasLimit(), api.FeeCap); err != nil {
		return common.Hash{}, err
	}

	if !txn.Protected() && !api.AllowUnprotectedTxs {
		return common.Hash{}, errors.New("only replay-protected (EIP-155) transactions allowed over RPC")
	}

	// this has been moved to prior to adding of transactions to capture the
	// pre-state of the db - which is used for logging in the messages below
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
		if chainId.Cmp(txnChainId) != 0 {
			return common.Hash{}, fmt.Errorf("invalid chain id, expected: %d got: %d", chainId, txnChainId)
		}
	}

	// [EIP-7594] Convert legacy (v0) blob sidecar to v1 cell proofs when Osaka is active.
	// This allows users with tooling that has not yet been updated for PeerDAS to still
	// submit blob transactions. The node computes the cell proofs on their behalf.
	// TODO: remove once ecosystem tooling fully supports wrapper_version=1.
	txBytes := []byte(encodedTx)
	if wrapper, ok := txn.(*types.BlobTxWrapper); ok && wrapper.WrapperVersion == 0 && cc.IsOsaka(uint64(time.Now().Unix())) {
		converted, err := wrapper.ConvertToV1()
		if err != nil {
			return common.Hash{}, fmt.Errorf("blob sidecar v0→v1 conversion failed: %w", err)
		}
		txBytes = converted
	}

	hash := txn.Hash()
	res, err := api.txPool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{txBytes}})
	if err != nil {
		return common.Hash{}, err
	}

	if res.Imported[0] != txpoolproto.ImportResult_SUCCESS {
		return hash, fmt.Errorf("%s: %s", txpoolproto.ImportResult_name[int32(res.Imported[0])], res.Errors[0])
	}

	return txn.Hash(), nil
}

// SendRawTransactionSync implements eth_sendRawTransactionSync (https://eips.ethereum.org/EIPS/eip-7966).
// Creates a new message call or contract creation for a previously signed transaction waiting for the transaction to be processed and the receipt to be available.
func (api *APIImpl) SendRawTransactionSync(ctx context.Context, encodedTx hexutil.Bytes, timeoutMs *uint64) (*ethutils.RPCReceipt, error) {
	// If timeout is not specified or zero, we use the default, otherwise we use the passed one capped by max.
	timeout := api.RpcTxSyncDefaultTimeout
	if timeoutMs != nil && *timeoutMs > 0 {
		reqTimeout := time.Duration(*timeoutMs) * time.Millisecond
		timeout = min(reqTimeout, api.RpcTxSyncMaxTimeout)
	}
	hash, err := api.SendRawTransaction(ctx, encodedTx)
	if err != nil {
		return nil, err
	}

	return api.waitForReceipt(ctx, hash, timeout)
}

func (api *APIImpl) waitForReceipt(ctx context.Context, hash common.Hash, timeout time.Duration) (*ethutils.RPCReceipt, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Subscribe to receive the receipt for the submitted transaction using the transaction hash.
	criteria := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{hash},
	}
	receiptsCh, id, err := api.filters.SubscribeReceipts(128, criteria)
	if err != nil {
		return nil, err
	}
	defer api.filters.UnsubscribeReceipts(id)

	// The filter update reaches the server asynchronously, so a receipt produced before it lands is never
	// delivered on the channel. Look the receipt up once to cover a transaction mined by then.
	// An exhausted budget is the timeout itself and is reported as such below; any other lookup failure is real.
	receipt, err := api.GetTransactionReceipt(timeoutCtx, hash)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	if receipt != nil {
		return receipt, nil
	}

	// Wait up to the timeout for the transaction to be processed and the receipt to be available.
	select {
	case <-timeoutCtx.Done():
		return nil, &rpc.TxSyncTimeoutError{
			Msg:  fmt.Sprintf("the transaction was added to the mempool but wasn't processed in %v", timeout),
			Hash: hash,
		}
	case batch, ok := <-receiptsCh:
		// The filter is this transaction's hash, so the batch holds only its receipt.
		if !ok || batch == nil || len(batch.Value) == 0 {
			log.Warn("[rpc] receipts subscription was closed")
			return nil, fmt.Errorf("receipts subscription was closed")
		}
		return ethutils.MarshalSubscribeReceipt(batch.Value[0]), nil
	}
}

// SendTransaction implements eth_sendTransaction. Creates new message call transaction or a contract creation if the data field contains code.
func (api *APIImpl) SendTransaction(_ context.Context, _ any) (common.Hash, error) {
	return common.Hash{0}, fmt.Errorf(NotImplemented, "eth_sendTransaction")
}

// checkTxFee is an internal function used to check whether the fee of
// the given transaction is _reasonable_(under the cap).
func checkTxFee(gasPrice *big.Int, gas uint64, gasCap float64) error {
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
