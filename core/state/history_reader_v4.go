package state

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

// HistoryReaderV4 Implements StateReader and StateWriter
type HistoryReaderV4 struct {
	ac    *libstate.AggregatorContext
	ri    *libstate.ReadIndices
	txNum uint64
	trace bool
	tx    kv.Tx
}

func NewHistoryReaderV4(ac *libstate.AggregatorContext, ri *libstate.ReadIndices) *HistoryReaderV4 {
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
	addrBytes := address.Bytes()
	if hr.ri != nil {
		if err := hr.ri.ReadAccountData(addrBytes); err != nil {
			return nil, err
		}
	}
	enc, err := hr.ac.ReadAccountDataBeforeTxNum(addrBytes, hr.txNum, hr.tx /* roTx */)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => []\n", addrBytes)
		}
		return nil, nil
	}
	var a accounts.Account
	if err := accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, fmt.Errorf("ReadAccountData(%x): %w", addrBytes, err)
	}

	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", addrBytes, a.Nonce, &a.Balance, a.CodeHash)
	}
	return &a, nil
}

func (hr *HistoryReaderV4) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	addrBytes, keyBytes := address.Bytes(), key.Bytes()
	if hr.ri != nil {
		if err := hr.ri.ReadAccountStorage(addrBytes, keyBytes); err != nil {
			return nil, err
		}
	}
	enc, err := hr.ac.ReadAccountStorageBeforeTxNum(addrBytes, keyBytes, hr.txNum, hr.tx /* roTx */)
	if err != nil {
		return nil, err
	}
	if hr.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] [%x] => []\n", addrBytes, keyBytes)
		} else {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [%x]\n", addrBytes, keyBytes, enc)
		}
	}
	if enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (hr *HistoryReaderV4) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	addrBytes := address.Bytes()
	if hr.ri != nil {
		if err := hr.ri.ReadAccountCode(addrBytes); err != nil {
			return nil, err
		}
	}
	enc, err := hr.ac.ReadAccountCodeBeforeTxNum(addrBytes, hr.txNum, nil /* roTx */)
	if err != nil {
		return nil, err
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x]\n", addrBytes, enc)
	}
	return enc, nil
}

func (hr *HistoryReaderV4) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	addrBytes := address.Bytes()
	if hr.ri != nil {
		if err := hr.ri.ReadAccountCodeSize(addrBytes); err != nil {
			return 0, err
		}
	}
	size, err := hr.ac.ReadAccountCodeSizeBeforeTxNum(addrBytes, hr.txNum, nil /* roTx */)
	if err != nil {
		return 0, err
	}
	if hr.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d]\n", addrBytes, size)
	}
	return size, nil
}

func (hr *HistoryReaderV4) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}
