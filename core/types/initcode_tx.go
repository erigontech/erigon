package types

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon/crypto"
)

type IniticodeTx struct {
	BlobTx
	initcodes [][]byte
}

func (txn *IniticodeTx) Type() byte { return InitcodeTxType }

func (txn *IniticodeTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg := Message{
		nonce:      txn.Nonce,
		gasLimit:   txn.Gas,
		gasPrice:   *txn.FeeCap,
		tip:        *txn.Tip,
		feeCap:     *txn.FeeCap,
		to:         txn.To,
		amount:     *txn.Value,
		data:       txn.Data,
		accessList: txn.AccessList,
		checkNonce: true,
	}
	if !rules.IsCancun {
		return msg, errors.New("BlobTx transactions require Cancun")
	}
	if baseFee != nil {
		overflow := msg.gasPrice.SetFromBig(baseFee)
		if overflow {
			return msg, fmt.Errorf("gasPrice higher than 2^256-1")
		}
	}
	msg.gasPrice.Add(&msg.gasPrice, txn.Tip)
	if msg.gasPrice.Gt(txn.FeeCap) {
		msg.gasPrice.Set(txn.FeeCap)
	}
	var err error
	msg.from, err = txn.Sender(s)
	msg.maxFeePerBlobGas = *txn.MaxFeePerBlobGas
	msg.blobHashes = txn.BlobVersionedHashes

	for _, initcode := range txn.initcodes {
		hash := crypto.Keccak256Hash(initcode)
		msg.initcodes[hash] = initcode
	}

	return msg, err
}
