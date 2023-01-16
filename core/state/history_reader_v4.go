package state

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"

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

// HistoryReaderV4 Implements StateReader and StateWriter
type HistoryReaderV4 struct {
	ac    *libstate.AggregatorContext
	ri    *libstate.ReadIndices
	txNum uint64
	trace bool
	tx    kv.Tx
}

func NewHistoryReader23(ac *libstate.AggregatorContext, ri *libstate.ReadIndices) *HistoryReaderV4 {
	return &HistoryReaderV4{ac: ac, ri: ri}
}

func (hr *HistoryReaderV4) SetTx(tx kv.Tx) { hr.tx = tx }

func (hr *HistoryReaderV4) SetRwTx(tx kv.RwTx) {
	hr.ri.SetTx(tx)
}

func (hr *HistoryReaderV4) SetTxNum(txNum uint64) {
	hr.txNum = txNum
	if hr.ri != nil {
		hr.ri.SetTxNum(txNum)
	}
}

func (hr *HistoryReaderV4) FinishTx() error {
	return hr.ri.FinishTx()
}

func (hr *HistoryReaderV4) SetTrace(trace bool) {
	hr.trace = trace
}

func (hr *HistoryReaderV4) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
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
	if err := accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, fmt.Errorf("ReadAccountData(%x): %w", address, err)
	}

	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
	}
	return &a, nil
}

func (hr *HistoryReaderV4) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
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

func (hr *HistoryReaderV4) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
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

func (hr *HistoryReaderV4) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
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

func (hr *HistoryReaderV4) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}
