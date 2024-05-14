package types

import libcommon "github.com/ledgerwatch/erigon-lib/common"

type SSZTransaction struct {
	Transacton
}

func (tx *SSZTransaction) Hash() libcommon.Hash {
	return libcommon.Hash{}
}

func (tx *SSZTransaction) Type() byte {
	return SSZTxType
}

func (tx *SSZTransaction) Unwrap() Transaction {
	return tx.Transaction
}

func (tx *SSZTransaction) copy() *SSZTransaction {
	return &SSZTransaction{
		Transaction: *tx.Transaction.copy(),
	}
}
