package core

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type RawTxPool TxPool

func RawFromTxPool(pool *TxPool) *RawTxPool {
	return (*RawTxPool)(pool)
}

func TxPoolFromRaw(pool *RawTxPool) *TxPool {
	return (*TxPool)(pool)
}

func (pool *RawTxPool) AddLocal(signedtx []byte) ([]byte, error) {
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(signedtx, tx); err != nil {
		return common.Hash{}.Bytes(), err
	}

	return tx.Hash().Bytes(), TxPoolFromRaw(pool).AddLocal(tx)
}
