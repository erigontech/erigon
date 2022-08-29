package state

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

// HistoryReader23 Implements StateReader and StateWriter
type HistoryReader23 struct {
	ac    *libstate.AggregatorContext
	ri    *libstate.ReadIndices
	txNum uint64
	trace bool
	tx    kv.Tx
}

func NewHistoryReader23(ac *libstate.AggregatorContext, ri *libstate.ReadIndices) *HistoryReader23 {
	return &HistoryReader23{ac: ac, ri: ri}
}

func (hr *HistoryReader23) SetTx(tx kv.Tx) { hr.tx = tx }

func (hr *HistoryReader23) SetRwTx(tx kv.RwTx) {
	hr.ri.SetTx(tx)
}

func (hr *HistoryReader23) SetTxNum(txNum uint64) {
	hr.txNum = txNum
	if hr.ri != nil {
		hr.ri.SetTxNum(txNum)
	}
}

func (hr *HistoryReader23) FinishTx() error {
	return hr.ri.FinishTx()
}

func (hr *HistoryReader23) SetTrace(trace bool) {
	hr.trace = trace
}

func (hr *HistoryReader23) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if hr.ri != nil {
		if err := hr.ri.ReadAccountData(address.Bytes()); err != nil {
			return nil, err
		}
	}
	enc, err := hr.ac.ReadAccountDataBeforeTxNum(address.Bytes(), hr.txNum, hr.tx /* roTx */)
	if err != nil {
		return nil, err
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

func (hr *HistoryReader23) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	if hr.ri != nil {
		if err := hr.ri.ReadAccountStorage(address.Bytes(), key.Bytes()); err != nil {
			return nil, err
		}
	}
	enc, err := hr.ac.ReadAccountStorageBeforeTxNum(address.Bytes(), key.Bytes(), hr.txNum, hr.tx /* roTx */)
	if err != nil {
		return nil, err
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

func (hr *HistoryReader23) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if hr.ri != nil {
		if err := hr.ri.ReadAccountCode(address.Bytes()); err != nil {
			return nil, err
		}
	}
	enc, err := hr.ac.ReadAccountCodeBeforeTxNum(address.Bytes(), hr.txNum, nil /* roTx */)
	if err != nil {
		return nil, err
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x]\n", address, enc)
	}
	return enc, nil
}

func (hr *HistoryReader23) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	if hr.ri != nil {
		if err := hr.ri.ReadAccountCodeSize(address.Bytes()); err != nil {
			return 0, err
		}
	}
	size, err := hr.ac.ReadAccountCodeSizeBeforeTxNum(address.Bytes(), hr.txNum, nil /* roTx */)
	if err != nil {
		return 0, err
	}
	if hr.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d]\n", address, size)
	}
	return size, nil
}

func (hr *HistoryReader23) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}
