package core

import (
	ethereum "github.com/ledgerwatch/turbo-geth"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
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
	SyncProgress() ethereum.SyncProgress
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

func (back *EthBackend) SyncProgress() (map[string]hexutil.Uint64, error) {
	progress := back.Backend.SyncProgress()
	return map[string]hexutil.Uint64{
		"startingBlock": hexutil.Uint64(progress.StartingBlock),
		"currentBlock":  hexutil.Uint64(progress.CurrentBlock),
		"highestBlock":  hexutil.Uint64(progress.HighestBlock),
		"pulledStates":  hexutil.Uint64(progress.PulledStates),
		"knownStates":   hexutil.Uint64(progress.KnownStates),
	}, nil
}
