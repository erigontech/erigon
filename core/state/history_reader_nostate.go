package state

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type RequiredStateError struct {
	stateTxNum uint64
}

func (r RequiredStateError) Error() string {
	return fmt.Sprintf("required state at txNum %d", r.stateTxNum)
}

// Implements StateReader and StateWriter
type HistoryReaderNoState struct {
	a      *libstate.Aggregator
	tx     kv.Tx
	txNum  uint64
	trace  bool
	bitmap *roaring64.Bitmap
}

func NewHistoryReaderNoState(a *libstate.Aggregator, bitmap *roaring64.Bitmap) *HistoryReaderNoState {
	return &HistoryReaderNoState{a: a, bitmap: bitmap}
}

func (hr *HistoryReaderNoState) SetTxNum(txNum uint64) {
	hr.txNum = txNum
	hr.a.SetTxNum(txNum)
}

func (hr *HistoryReaderNoState) SetTx(tx kv.Tx) {
	hr.tx = tx
}

func (hr *HistoryReaderNoState) SetTrace(trace bool) {
	hr.trace = trace
}

func (hr *HistoryReaderNoState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, noState, stateTxNum, err := hr.a.ReadAccountDataNoState(address.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !noState {
		if !hr.bitmap.Contains(stateTxNum) {
			return nil, RequiredStateError{stateTxNum: stateTxNum}
		}
		enc, err = hr.tx.GetOne(kv.PlainState, address.Bytes())
		if err != nil {
			return nil, err
		}
		var a accounts.Account
		if err = a.DecodeForStorage(enc); err != nil {
			return nil, err
		}
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
		}
		return &a, nil
	}
	if len(enc) == 0 {
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => []\n", address)
		}
		return nil, nil
	}
	var a accounts.Account
	a.Reset()
	pos := 0
	nonceBytes := int(enc[pos])
	pos++
	if nonceBytes > 0 {
		a.Nonce = bytesToUint64(enc[pos : pos+nonceBytes])
		pos += nonceBytes
	}
	balanceBytes := int(enc[pos])
	pos++
	if pos+balanceBytes >= len(enc) {
		fmt.Printf("panic ReadAccountData(%x)=>[%x]\n", address, enc)
	}
	if balanceBytes > 0 {
		a.Balance.SetBytes(enc[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	codeHashBytes := int(enc[pos])
	pos++
	if codeHashBytes > 0 {
		copy(a.CodeHash[:], enc[pos:pos+codeHashBytes])
		pos += codeHashBytes
	}
	if pos >= len(enc) {
		fmt.Printf("panic ReadAccountData(%x)=>[%x]\n", address, enc)
	}
	incBytes := int(enc[pos])
	pos++
	if incBytes > 0 {
		a.Incarnation = bytesToUint64(enc[pos : pos+incBytes])
	}
	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
	}
	return &a, nil
}

func (hr *HistoryReaderNoState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	enc, noState, stateTxNum, err := hr.a.ReadAccountStorageNoState(address.Bytes(), key.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !noState {
		if !hr.bitmap.Contains(stateTxNum) {
			return nil, RequiredStateError{stateTxNum: stateTxNum}
		}
		compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), FirstContractIncarnation, key.Bytes())
		enc, err = hr.tx.GetOne(kv.PlainState, compositeKey)
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

func (hr *HistoryReaderNoState) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	enc, noState, stateTxNum, err := hr.a.ReadAccountCodeNoState(address.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !noState {
		if !hr.bitmap.Contains(stateTxNum) {
			return nil, RequiredStateError{stateTxNum: stateTxNum}
		}
		enc, err = hr.tx.GetOne(kv.Code, codeHash.Bytes())
		if err != nil {
			return nil, err
		}
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x]\n", address, enc)
	}
	return enc, nil
}

func (hr *HistoryReaderNoState) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	size, noState, stateTxNum, err := hr.a.ReadAccountCodeSizeNoState(address.Bytes(), hr.txNum)
	if err != nil {
		return 0, err
	}
	if !noState {
		if !hr.bitmap.Contains(stateTxNum) {
			return 0, RequiredStateError{stateTxNum: stateTxNum}
		}
		enc, err := hr.tx.GetOne(kv.Code, codeHash.Bytes())
		if err != nil {
			return 0, err
		}
		size = len(enc)
	}
	if hr.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d]\n", address, size)
	}
	return size, nil
}

func (hr *HistoryReaderNoState) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}
