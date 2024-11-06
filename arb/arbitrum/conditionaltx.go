package arbitrum

import (
	"context"
	"errors"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon/turbo/jsonrpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/arb/arbitrum_types"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
)

type ArbTransactionAPI struct {
	b *APIBackend
}

func NewArbTransactionAPI(b *APIBackend) *ArbTransactionAPI {
	return &ArbTransactionAPI{b}
}

func (s *ArbTransactionAPI) SendRawTransactionConditional(ctx context.Context, input hexutil.Bytes, options *arbitrum_types.ConditionalOptions) (common.Hash, error) {
	tx := new(types.ArbTx)
	if err := tx.UnmarshalBinary(input); err != nil {
		return common.Hash{}, err
	}
	return SubmitConditionalTransaction(ctx, s.b, tx, options)
}

func SubmitConditionalTransaction(ctx context.Context, b *APIBackend, tx *types.ArbTx, options *arbitrum_types.ConditionalOptions) (common.Hash, error) {
	// If the transaction fee cap is already specified, ensure the
	// fee of the given transaction is _reasonable_.
	if err := jsonrpc.CheckTxFee(tx.GasPrice(), tx.Gas(), b.RPCTxFeeCap()); err != nil {
		return common.Hash{}, err
	}
	if !b.UnprotectedAllowed() && !tx.Protected() {
		// Ensure only eip155 signed transactions are submitted if EIP155Required is set.
		return common.Hash{}, errors.New("only replay-protected (EIP-155) transactions allowed over RPC")
	}
	if err := b.SendConditionalTx(ctx, tx, options); err != nil {
		return common.Hash{}, err
	}
	// Print a log with full tx details for manual investigations and interventions
	signer := types.MakeSigner(b.ChainConfig(), b.CurrentBlock().Number.Uint64(), b.CurrentBlock().Time)
	from, err := types.Sender(signer, tx)
	if err != nil {
		return common.Hash{}, err
	}

	if tx.To() == nil {
		addr := crypto.CreateAddress(from, tx.Nonce())
		log.Info("Submitted contract creation", "hash", tx.Hash().Hex(), "from", from, "nonce", tx.Nonce(), "contract", addr.Hex(), "value", tx.Value())
	} else {
		log.Info("Submitted transaction", "hash", tx.Hash().Hex(), "from", from, "nonce", tx.Nonce(), "recipient", tx.To(), "value", tx.Value())
	}
	return tx.Hash(), nil
}

func SendConditionalTransactionRPC(ctx context.Context, rpc *rpc.Client, tx types.ArbTx, options *arbitrum_types.ConditionalOptions) error {
	data, err := tx.MarshalBinary()
	if err != nil {
		return err
	}
	return rpc.CallContext(ctx, nil, "eth_sendRawTransactionConditional", hexutility.Encode(data), options)
}
