package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// SendRawTransaction implements eth_sendRawTransaction. It submits a signed
// transaction to the pool and returns its hash on successful admission.
func (api *APIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	if err := txpool.ValidateSerializedTxn(encodedTx); err != nil {
		return common.Hash{}, err
	}
	txn, err := types.DecodeWrappedTransaction(encodedTx)
	if err != nil {
		return common.Hash{}, err
	}

	if err := checkTxFee(txn, api.FeeCap); err != nil {
		return common.Hash{}, err
	}

	if !txn.Protected() && !api.AllowUnprotectedTxs {
		return common.Hash{}, errors.New("only replay-protected (EIP-155) transactions allowed over RPC")
	}

	// Admission opens its own state view; release this database reader before calling it.
	cc, err := func() (*chain.Config, error) {
		tx, err := api.db.BeginTemporalRo(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		return api.chainConfig(ctx, tx)
	}()
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
// It submits a signed transaction and waits for its receipt until the timeout expires.
func (api *APIImpl) SendRawTransactionSync(ctx context.Context, encodedTx hexutil.Bytes, timeoutMs *uint64) (*ethutils.RPCReceipt, error) {
	hash, err := api.SendRawTransaction(ctx, encodedTx)
	if err != nil {
		return nil, err
	}

	return api.waitForReceipt(ctx, hash, api.txSyncTimeout(timeoutMs))
}

// txSyncTimeout returns the requested timeout capped by the maximum, or the default when none is requested.
func (api *APIImpl) txSyncTimeout(timeoutMs *uint64) time.Duration {
	if timeoutMs == nil || *timeoutMs == 0 {
		return api.RpcTxSyncDefaultTimeout
	}
	if *timeoutMs > uint64(api.RpcTxSyncMaxTimeout.Milliseconds()) { // in ms: a huge request overflows a Duration
		return api.RpcTxSyncMaxTimeout
	}
	return time.Duration(*timeoutMs) * time.Millisecond
}

const receiptPollInterval = time.Second

func (api *APIImpl) waitForReceipt(ctx context.Context, hash common.Hash, timeout time.Duration) (*ethutils.RPCReceipt, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Subscribe to receive the receipt for the submitted transaction using the transaction hash.
	criteria := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{hash},
	}
	receiptsCh, id, err := api.filters.SubscribeReceipts(128, criteria)
	switch {
	case err == nil:
		defer api.filters.UnsubscribeReceipts(id)
	case errors.Is(err, rpchelper.ErrTooManySubscriptions):
		// The poll below carries the wait on its own; the nil channel never fires.
	default:
		return nil, err
	}

	// The filter update reaches the server asynchronously and without an ack, so a receipt produced before it
	// lands is never delivered on the channel. Look the receipt up periodically to cover that window.
	poll := time.NewTicker(receiptPollInterval)
	defer poll.Stop()
	for {
		// An exhausted budget is the timeout itself and is reported as such below; any other lookup failure is real.
		receipt, err := api.GetTransactionReceipt(timeoutCtx, hash)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if receipt != nil {
			return receipt, nil
		}

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
		case <-poll.C:
		}
	}
}

// SendTransaction implements eth_sendTransaction. Creates new message call transaction or a contract creation if the data field contains code.
func (api *APIImpl) SendTransaction(_ context.Context, _ any) (common.Hash, error) {
	return common.Hash{0}, fmt.Errorf(NotImplemented, "eth_sendTransaction")
}

// checkTxFee enforces --rpc.txfeecap, the maximum transaction fee allowed over RPC
// in ether. A fee equal to the cap is allowed. A zero cap disables the check.
func checkTxFee(txn types.Transaction, capEther float64) error {
	if capEther == 0 {
		return nil
	}

	var feeWei *big.Int
	if frame, ok := txn.Unwrap().(*types.FrameTransaction); ok {
		// MaxFee is the signed budget for the whole transaction, including blob fees.
		// GetFeeCap rounds down to whole wei per gas, so multiplying by the gas
		// limit can understate that budget.
		feeWei = frame.Fees.MaxFee.ToBig()
	} else {
		feeWei = new(big.Int).Mul(txn.GetFeeCap().ToBig(), new(big.Int).SetUint64(txn.GetGasLimit()))
	}
	// Read the float64 setting as a decimal ether amount so a cap such as 0.3
	// compares exactly. Rounding the fee to float64 could hide a one-wei excess.
	feeEther := new(big.Rat).SetFrac(feeWei, big.NewInt(common.Ether))
	limit, finite := new(big.Rat).SetString(strconv.FormatFloat(capEther, 'g', -1, 64))
	// SetString rejects infinities and NaN. Of those, only -Inf is smaller than a fee.
	if capEther < 0 || finite && feeEther.Cmp(limit) > 0 {
		return fmt.Errorf("tx fee (%s ether) exceeds the configured cap (%g ether)", feeEther.FloatString(18), capEther)
	}
	return nil
}
