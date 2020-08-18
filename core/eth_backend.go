package core

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type EthBackend struct {
	Backend
}

type Backend interface {
	TxPool() *TxPool
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
}

func NewEthBackend(eth Backend) *EthBackend {
	return &EthBackend{eth}
}

func (back *EthBackend) AddLocal(signedtx []byte) ([]byte, error) {
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(signedtx, tx); err != nil {
		return common.Hash{}.Bytes(), err
	}

	return tx.Hash().Bytes(), back.TxPool().AddLocal(tx)
}
