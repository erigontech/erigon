package types

import libcommon "github.com/ledgerwatch/erigon-lib/common"

type SSZTransaction struct {
	BlobTx
}

func (tx *SSZTransaction) Hash() libcommon.Hash {
	return libcommon.Hash{}
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
