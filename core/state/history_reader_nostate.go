package state

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type RequiredStateError struct {
	StateTxNum uint64
}

func (r *RequiredStateError) Error() string {
	return fmt.Sprintf("required state at txNum %d", r.StateTxNum)
}

type HistoryReaderNoState struct {
	ac         *libstate.Aggregator22Context
	tx         kv.Tx
	txNum      uint64
	trace      bool
	rs         *ReconState
	readError  bool
	stateTxNum uint64
	composite  []byte
}

func NewHistoryReaderNoState(ac *libstate.Aggregator22Context, rs *ReconState) *HistoryReaderNoState {
	return &HistoryReaderNoState{ac: ac, rs: rs}
}

func (hr *HistoryReaderNoState) SetTxNum(txNum uint64) {
	hr.txNum = txNum
}

func (hr *HistoryReaderNoState) SetTx(tx kv.Tx) {
	hr.tx = tx
}

func (hr *HistoryReaderNoState) SetTrace(trace bool) {
	hr.trace = trace
}

func (hr *HistoryReaderNoState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addr := address.Bytes()
	txKey, err := hr.tx.GetOne(kv.XAccount, addr)
	if err != nil {
		return nil, err
	}
	var stateTxNum uint64 = math.MaxUint64
	if txKey != nil {
		stateTxNum = binary.BigEndian.Uint64(txKey)
	}
	var enc []byte
	noState := false
	if stateTxNum >= hr.txNum {
		if enc, noState, err = hr.ac.ReadAccountDataNoState(addr, hr.txNum); err != nil {
			return nil, err
		}
	}
	if !noState {
		if txKey == nil {
			return nil, nil
		}
		if !hr.rs.Done(stateTxNum) {
			hr.readError = true
			hr.stateTxNum = stateTxNum
			return nil, &RequiredStateError{StateTxNum: stateTxNum}
		}
		enc = hr.rs.Get(kv.PlainStateR, addr, nil, stateTxNum)
		if enc == nil {
			if hr.tx == nil {
				return nil, fmt.Errorf("hr.tx is nil")
			}
			if cap(hr.composite) < 8+20 {
				hr.composite = make([]byte, 8+20)
			} else if len(hr.composite) != 8+20 {
				hr.composite = hr.composite[:8+20]
			}
			binary.BigEndian.PutUint64(hr.composite, stateTxNum)
			copy(hr.composite[8:], addr)
			enc, err = hr.tx.GetOne(kv.PlainStateR, hr.composite)
			if err != nil {
				return nil, err
			}
			if enc == nil {
				return nil, nil
			}
		}
		var a accounts.Account
		if err = a.DecodeForStorage(enc); err != nil {
			return nil, err
		}
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], noState=%t, stateTxNum=%d, txNum: %d\n", address, a.Nonce, &a.Balance, a.CodeHash, noState, stateTxNum, hr.txNum)
		}
		return &a, nil
	}
	if len(enc) == 0 {
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => [], noState=%t, txNum: %d\n", address, noState, hr.txNum)
		}
		return nil, nil
	}
	var a accounts.Account
	if err = accounts.Deserialise2(&a, enc); err != nil {
		return nil, err
	}
	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], noState=%t, txNum: %d\n", address, a.Nonce, &a.Balance, a.CodeHash, noState, hr.txNum)
	}
	return &a, nil
}

func (hr *HistoryReaderNoState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	if cap(hr.composite) < 20+32 {
		hr.composite = make([]byte, 20+32)
	} else {
		hr.composite = hr.composite[:20+32]
	}
	addr, k := address.Bytes(), key.Bytes()
	copy(hr.composite, addr)
	copy(hr.composite[20:], k)
	txKey, err := hr.tx.GetOne(kv.XStorage, hr.composite)
	if err != nil {
		return nil, err
	}
	var stateTxNum uint64 = math.MaxUint64
	if txKey != nil {
		stateTxNum = binary.BigEndian.Uint64(txKey)
	}
	var enc []byte
	noState := false
	if stateTxNum >= hr.txNum {
		if enc, noState, err = hr.ac.ReadAccountStorageNoState(addr, k, hr.txNum); err != nil {
			return nil, err
		}
	}
	if !noState {
		if txKey == nil {
			return nil, nil
		}
		if !hr.rs.Done(stateTxNum) {
			hr.readError = true
			hr.stateTxNum = stateTxNum
			return nil, &RequiredStateError{StateTxNum: stateTxNum}
		}

		enc = hr.rs.Get(kv.PlainStateR, addr, k, stateTxNum)
		if enc == nil {
			if hr.tx == nil {
				return nil, fmt.Errorf("hr.tx is nil")
			}
			if cap(hr.composite) < 8+20+8+32 {
				hr.composite = make([]byte, 8+20+8+32)
			} else if len(hr.composite) != 8+20+8+32 {
				hr.composite = hr.composite[:8+20+8+32]
			}
			binary.BigEndian.PutUint64(hr.composite, stateTxNum)
			copy(hr.composite[8:], addr)
			binary.BigEndian.PutUint64(hr.composite[8+20:], 1)
			copy(hr.composite[8+20+8:], k)
			enc, err = hr.tx.GetOne(kv.PlainStateR, hr.composite)
			if err != nil {
				return nil, err
			}
		}
	}
	if hr.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [], txNum: %d\n", address, key.Bytes(), hr.txNum)
		} else {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [%x], txNum: %d\n", address, key.Bytes(), enc, hr.txNum)
		}
	}
	if enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (hr *HistoryReaderNoState) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	addr := address.Bytes()
	txKey, err := hr.tx.GetOne(kv.XCode, addr)
	if err != nil {
		return nil, err
	}
	var stateTxNum uint64 = math.MaxUint64
	if txKey != nil {
		stateTxNum = binary.BigEndian.Uint64(txKey)
	}
	var enc []byte
	noState := false
	if stateTxNum >= hr.txNum {
		if enc, noState, err = hr.ac.ReadAccountCodeNoState(addr, hr.txNum); err != nil {
			return nil, err
		}
	}
	if !noState {
		if txKey == nil {
			return nil, nil
		}
		if !hr.rs.Done(stateTxNum) {
			hr.readError = true
			hr.stateTxNum = stateTxNum
			return nil, &RequiredStateError{StateTxNum: stateTxNum}
		}
		cHash := codeHash.Bytes()
		enc = hr.rs.Get(kv.CodeR, cHash, nil, stateTxNum)
		if enc == nil {
			if hr.tx == nil {
				fmt.Printf("ReadAccountCode [%x] %d\n", address, incarnation)
				return nil, fmt.Errorf("hr.tx is nil")
			}
			if cap(hr.composite) < 8+32 {
				hr.composite = make([]byte, 8+32)
			} else if len(hr.composite) != 8+32 {
				hr.composite = hr.composite[:8+32]
			}
			binary.BigEndian.PutUint64(hr.composite, stateTxNum)
			copy(hr.composite[8:], cHash)
			enc, err = hr.tx.GetOne(kv.CodeR, hr.composite)
			if err != nil {
				return nil, err
			}
		}
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], noState=%t, txNum: %d\n", address, enc, noState, hr.txNum)
	}
	return enc, nil
}

func (hr *HistoryReaderNoState) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	addr := address.Bytes()
	txKey, err := hr.tx.GetOne(kv.XCode, addr)
	if err != nil {
		return 0, err
	}
	var stateTxNum uint64 = math.MaxUint64
	if txKey != nil {
		stateTxNum = binary.BigEndian.Uint64(txKey)
	}
	var size int
	noState := false
	if stateTxNum >= hr.txNum {
		if size, noState, err = hr.ac.ReadAccountCodeSizeNoState(addr, hr.txNum); err != nil {
			return 0, err
		}
	}
	if !noState {
		if txKey == nil {
			return 0, nil
		}
		if !hr.rs.Done(stateTxNum) {
			hr.readError = true
			hr.stateTxNum = stateTxNum
			return 0, &RequiredStateError{StateTxNum: stateTxNum}
		}
		cHash := codeHash.Bytes()
		enc := hr.rs.Get(kv.CodeR, cHash, nil, stateTxNum)
		if enc == nil {
			if cap(hr.composite) < 8+32 {
				hr.composite = make([]byte, 8+32)
			} else if len(hr.composite) != 8+32 {
				hr.composite = hr.composite[:8+32]
			}
			binary.BigEndian.PutUint64(hr.composite, stateTxNum)
			copy(hr.composite[8:], cHash)
			enc, err = hr.tx.GetOne(kv.CodeR, hr.composite)
			if err != nil {
				return 0, err
			}
		}
		size = len(enc)
	}
	if hr.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d], txNum: %d\n", address, size, hr.txNum)
	}
	return size, nil
}

func (hr *HistoryReaderNoState) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

func (hr *HistoryReaderNoState) ResetError() {
	hr.readError = false
}

func (hr *HistoryReaderNoState) ReadError() (uint64, bool) {
	return hr.stateTxNum, hr.readError
}
