package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	txPoolProto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

// SendRawTransaction implements eth_sendRawTransaction. Creates new message call transaction or a contract creation for previously-signed transactions.
func (api *APIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (libcommon.Hash, error) {
	txn, err := types.DecodeTransaction(rlp.NewStream(bytes.NewReader(encodedTx), uint64(len(encodedTx))))
	if err != nil {
		return libcommon.Hash{}, err
	}

	// If the transaction fee cap is already specified, ensure the
	// fee of the given transaction is _reasonable_.
	if err := checkTxFee(txn.GetPrice().ToBig(), txn.GetGas(), ethconfig.Defaults.RPCTxFeeCap); err != nil {
		return libcommon.Hash{}, err
	}
	if !txn.Protected() {
		return libcommon.Hash{}, errors.New("only replay-protected (EIP-155) transactions allowed over RPC")
	}
	hash := txn.Hash()
	res, err := api.txPool.Add(ctx, &txPoolProto.AddRequest{RlpTxs: [][]byte{encodedTx}})
	if err != nil {
		return libcommon.Hash{}, err
	}

	if res.Imported[0] != txPoolProto.ImportResult_SUCCESS {
		return hash, fmt.Errorf("%s: %s", txPoolProto.ImportResult_name[int32(res.Imported[0])], res.Errors[0])
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return libcommon.Hash{}, err
	}
	defer tx.Rollback()

	// Print a log with full txn details for manual investigations and interventions
	blockNum := rawdb.ReadCurrentBlockNumber(tx)
	if blockNum == nil {
		return libcommon.Hash{}, err
	}
	cc, err := api.chainConfig(tx)
	if err != nil {
		return libcommon.Hash{}, err
	}

	txnChainId := txn.GetChainID()
	chainId := cc.ChainID

	if chainId.Cmp(txnChainId.ToBig()) != 0 {
		return libcommon.Hash{}, fmt.Errorf("invalid chain id, expected: %d got: %d", chainId, *txnChainId)
	}

	signer := types.MakeSigner(cc, *blockNum)
	from, err := txn.Sender(*signer)
	if err != nil {
		return libcommon.Hash{}, err
	}

	if txn.GetTo() == nil {
		addr := crypto.CreateAddress(from, txn.GetNonce())
		log.Info("Submitted contract creation", "hash", txn.Hash().Hex(), "from", from, "nonce", txn.GetNonce(), "contract", addr.Hex(), "value", txn.GetValue())
	} else {
		log.Info("Submitted transaction", "hash", txn.Hash().Hex(), "from", from, "nonce", txn.GetNonce(), "recipient", txn.GetTo(), "value", txn.GetValue())
	}

	return txn.Hash(), nil
}

// SendTransaction implements eth_sendTransaction. Creates new message call transaction or a contract creation if the data field contains code.
func (api *APIImpl) SendTransaction(_ context.Context, txObject interface{}) (libcommon.Hash, error) {
	return libcommon.Hash{0}, fmt.Errorf(NotImplemented, "eth_sendTransaction")
}

// checkTxFee is an internal function used to check whether the fee of
// the given transaction is _reasonable_(under the cap).
func checkTxFee(gasPrice *big.Int, gas uint64, cap float64) error {
	// Short circuit if there is no cap for transaction fee at all.
	if cap == 0 {
		return nil
	}
	feeEth := new(big.Float).Quo(new(big.Float).SetInt(new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gas))), new(big.Float).SetInt(big.NewInt(params.Ether)))
	feeFloat, _ := feeEth.Float64()
	if feeFloat > cap {
		return fmt.Errorf("tx fee (%.2f ether) exceeds the configured cap (%.2f ether)", feeFloat, cap)
	}
	return nil
}
