package state

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

// HistoryReader22 Implements StateReader and StateWriter
type HistoryReader22 struct {
	ac    *libstate.Aggregator22Context
	txNum uint64
	trace bool
	tx    kv.Tx
}

func NewHistoryReader22(ac *libstate.Aggregator22Context) *HistoryReader22 {
	return &HistoryReader22{ac: ac}
}

func (hr *HistoryReader22) SetTx(tx kv.Tx)        { hr.tx = tx }
func (hr *HistoryReader22) SetTxNum(txNum uint64) { hr.txNum = txNum }
func (hr *HistoryReader22) SetTrace(trace bool)   { hr.trace = trace }

func (hr *HistoryReader22) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, ok, err := hr.ac.ReadAccountDataNoStateWithRecent(address.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		enc, err = hr.tx.GetOne(kv.PlainState, address.Bytes())
		if err != nil {
			return nil, err
		}
	}
	if len(enc) == 0 {
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => []\n", address)
		}
		return nil, nil
	}
	var a accounts.Account
	if err := accounts.Deserialise2(&a, enc); err != nil {
		return nil, fmt.Errorf("ReadAccountData(%x): %w", address, err)
	}

	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
	}
	return &a, nil
}

func (hr *HistoryReader22) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	enc, ok, err := hr.ac.ReadAccountStorageNoStateWithRecent(address.Bytes(), key.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		k := append(address.Bytes(), key.Bytes()...)
		enc, err = hr.tx.GetOne(kv.PlainState, k)
		if err != nil {
			return nil, err
		}
	}
	if hr.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] [%x] => []\n", address, key.Bytes())
		} else {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [%x]\n", address, key.Bytes(), enc)
		}
	}
	if enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (hr *HistoryReader22) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	enc, ok, err := hr.ac.ReadAccountCodeNoStateWithRecent(address.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		enc, err = hr.tx.GetOne(kv.Code, address.Bytes())
		if err != nil {
			return nil, err
		}
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x]\n", address, enc)
	}
	return enc, nil
}

func (hr *HistoryReader22) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	size, ok, err := hr.ac.ReadAccountCodeSizeNoStateWithRecent(address.Bytes(), hr.txNum)
	if err != nil {
		return 0, err
	}
	if !ok {
		enc, err := hr.tx.GetOne(kv.Code, address.Bytes())
		if err != nil {
			return 0, err
		}
		size = len(enc)
	}
	if err != nil {
		return 0, err
	}
	if hr.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d]\n", address, size)
	}
	return size, nil
}

func (hr *HistoryReader22) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}
