package state

import (
	"encoding/binary"
	"fmt"

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
	enc, noState, stateTxNum, err := hr.ac.ReadAccountDataNoState(address.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !noState {
		if !hr.rs.Done(stateTxNum) {
			hr.readError = true
			hr.stateTxNum = stateTxNum
			return nil, &RequiredStateError{StateTxNum: stateTxNum}
		}
		enc = hr.rs.Get(kv.PlainStateR, address.Bytes(), nil, stateTxNum)
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
			copy(hr.composite[8:], address.Bytes())
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
			fmt.Printf("ReadAccountData [%x] => [], noState=%t, stateTxNum=%d, txNum: %d\n", address, noState, stateTxNum, hr.txNum)
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
	incBytes := int(enc[pos])
	pos++
	if incBytes > 0 {
		a.Incarnation = bytesToUint64(enc[pos : pos+incBytes])
	}
	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x], noState=%t, stateTxNum=%d, txNum: %d\n", address, a.Nonce, &a.Balance, a.CodeHash, noState, stateTxNum, hr.txNum)
	}
	return &a, nil
}

func (hr *HistoryReaderNoState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	enc, noState, stateTxNum, err := hr.ac.ReadAccountStorageNoState(address.Bytes(), key.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !noState {
		if !hr.rs.Done(stateTxNum) {
			hr.readError = true
			hr.stateTxNum = stateTxNum
			return nil, &RequiredStateError{StateTxNum: stateTxNum}
		}

		enc = hr.rs.Get(kv.PlainStateR, address.Bytes(), key.Bytes(), stateTxNum)
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
			copy(hr.composite[8:], address.Bytes())
			binary.BigEndian.PutUint64(hr.composite[8+20:], 1)
			copy(hr.composite[8+20+8:], key.Bytes())
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
	enc, noState, stateTxNum, err := hr.ac.ReadAccountCodeNoState(address.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !noState {
		if !hr.rs.Done(stateTxNum) {
			hr.readError = true
			hr.stateTxNum = stateTxNum
			return nil, &RequiredStateError{StateTxNum: stateTxNum}
		}
		enc = hr.rs.Get(kv.CodeR, codeHash.Bytes(), nil, stateTxNum)
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
			copy(hr.composite[8:], codeHash.Bytes())
			enc, err = hr.tx.GetOne(kv.CodeR, hr.composite)
			if err != nil {
				return nil, err
			}
		}
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], noState=%t, stateTxNum=%d, txNum: %d\n", address, enc, noState, stateTxNum, hr.txNum)
	}
	return enc, nil
}

func (hr *HistoryReaderNoState) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	size, noState, stateTxNum, err := hr.ac.ReadAccountCodeSizeNoState(address.Bytes(), hr.txNum)
	if err != nil {
		return 0, err
	}
	if !noState {
		if !hr.rs.Done(stateTxNum) {
			hr.readError = true
			hr.stateTxNum = stateTxNum
			return 0, &RequiredStateError{StateTxNum: stateTxNum}
		}
		enc := hr.rs.Get(kv.CodeR, codeHash.Bytes(), nil, stateTxNum)
		if enc == nil {
			if cap(hr.composite) < 8+32 {
				hr.composite = make([]byte, 8+32)
			} else if len(hr.composite) != 8+32 {
				hr.composite = hr.composite[:8+32]
			}
			binary.BigEndian.PutUint64(hr.composite, stateTxNum)
			copy(hr.composite[8:], codeHash.Bytes())
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
