package types

import (
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type SSZTransaction struct {
	BlobTx
}

func (tx *SSZTransaction) Hash() libcommon.Hash {
	hash, _ := tx.AsSignedTransation().txHash()
	return libcommon.Hash(hash)
}

func (tx *SSZTransaction) Type() byte {
	return SSZTxType
}

func (tx *SSZTransaction) Unwrap() Transaction {
	return tx
}

func (tx *SSZTransaction) copy() *SSZTransaction {
	return &SSZTransaction{
		BlobTx: *tx.BlobTx.copy(),
	}
}

func (tx *SSZTransaction) AsSignedTransation() *SignedTransaction {
	return &SignedTransaction{*tx.AsTransationPayload(), TransactionSignature{}}
}

func (tx *SSZTransaction) AsTransationPayload() *TransactionPayload {
	return &TransactionPayload{
		Type:                sszType(tx),
		ChainID:             tx.ChainID,
		Nonce:               tx.Nonce,
		GasPrice:            gasPrice(tx),
		Gas:                 tx.Gas,
		To:                  tx.To,
		Value:               tx.Value,
		Input:               tx.Data,
		Accesses:            &tx.AccessList,
		Tip:                 tx.Tip,
		MaxFeePerBlobGas:    tx.MaxFeePerBlobGas,
		BlobVersionedHashes: &tx.BlobVersionedHashes,
	}
}

func sszType(tx Transaction) byte {
	//TODO: This needs to be refined
	return tx.Type()
}

func gasPrice(tx Transaction) *uint256.Int {
	switch tx.Unwrap().Type() {
	case LegacyTxType:
		return tx.GetPrice()
	default:
		return tx.GetFeeCap()
	}
}
