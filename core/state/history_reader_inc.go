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

type HistoryReaderInc struct {
	as         *libstate.AggregatorStep
	tx         kv.Tx
	chainTx    kv.Tx
	txNum      uint64
	trace      bool
	rs         *ReconState
	readError  bool
	stateTxNum uint64
	composite  []byte
}

func NewHistoryReaderInc(as *libstate.AggregatorStep, rs *ReconState) *HistoryReaderInc {
	return &HistoryReaderInc{as: as, rs: rs}
}

func (hr *HistoryReaderInc) SetTxNum(txNum uint64) {
	hr.txNum = txNum
}

func (hr *HistoryReaderInc) SetTx(tx kv.Tx) {
	hr.tx = tx
}

func (hr *HistoryReaderInc) SetChainTx(chainTx kv.Tx) {
	hr.chainTx = chainTx
}

func (hr *HistoryReaderInc) SetTrace(trace bool) {
	hr.trace = trace
}

func (hr *HistoryReaderInc) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addr := address.Bytes()
	txKey, err := hr.tx.GetOne(kv.XAccount, addr)
	if err != nil {
		return nil, err
	}
	var stateTxNum uint64 = math.MaxUint64
	if txKey != nil {
		stateTxNum = binary.BigEndian.Uint64(txKey)
	}
	if hr.trace {
		fmt.Printf("ReadAccountData [%x]=> hr.txNum=%d, stateTxNum=%d\n", address, hr.txNum, stateTxNum)
	}
	var enc []byte
	noState := false
	if txKey != nil && stateTxNum >= hr.txNum {
		if enc, noState, err = hr.as.ReadAccountDataNoState(addr, hr.txNum); err != nil {
			return nil, err
		}
		if hr.trace {
			fmt.Printf("ReadAccountData [%x]=> hr.txNum=%d, noState=%t\n", address, hr.txNum, noState)
		}
	}
	if !noState {
		if txKey == nil {
			enc, err = hr.chainTx.GetOne(kv.PlainState, addr)
			if err != nil {
				return nil, err
			}
		} else {
			if !hr.rs.Done(stateTxNum) {
				hr.readError = true
				hr.stateTxNum = stateTxNum
				return nil, &RequiredStateError{StateTxNum: stateTxNum}
			}
			enc = hr.rs.Get(kv.PlainStateR, addr, nil, stateTxNum)
			if hr.trace {
				fmt.Printf("ReadAccountData [%x]=> hr.txNum=%d, enc=%x\n", address, hr.txNum, enc)
			}
			if enc == nil {
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
			}
		}
		if enc == nil {
			return nil, nil
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

func (hr *HistoryReaderInc) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
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
	if hr.trace {
		fmt.Printf("ReadAccountStorage [%x] [%x] => hr.txNum=%d, stateTxNum=%d\n", address, key.Bytes(), hr.txNum, stateTxNum)
	}
	var enc []byte
	noState := false
	if txKey != nil && stateTxNum >= hr.txNum {
		if enc, noState, err = hr.as.ReadAccountStorageNoState(addr, k, hr.txNum); err != nil {
			return nil, err
		}
	}
	if !noState {
		if txKey == nil {
			if cap(hr.composite) < 20+8+32 {
				hr.composite = make([]byte, 20+8+32)
			} else if len(hr.composite) != 20+8+32 {
				hr.composite = hr.composite[:20+8+32]
			}
			copy(hr.composite, addr)
			binary.BigEndian.PutUint64(hr.composite[20:], FirstContractIncarnation)
			copy(hr.composite[20+8:], k)
			enc, err = hr.chainTx.GetOne(kv.PlainState, hr.composite)
			if err != nil {
				return nil, err
			}
		} else {
			if !hr.rs.Done(stateTxNum) {
				hr.readError = true
				hr.stateTxNum = stateTxNum
				return nil, &RequiredStateError{StateTxNum: stateTxNum}
			}
			enc = hr.rs.Get(kv.PlainStateR, addr, k, stateTxNum)
			if hr.trace {
				fmt.Printf("ReadAccountStorage [%x] [%x] => hr.txNum=%d, enc=%x\n", address, key.Bytes(), hr.txNum, enc)
			}
			if enc == nil {
				if cap(hr.composite) < 8+20+8+32 {
					hr.composite = make([]byte, 8+20+8+32)
				} else if len(hr.composite) != 8+20+8+32 {
					hr.composite = hr.composite[:8+20+8+32]
				}
				binary.BigEndian.PutUint64(hr.composite, stateTxNum)
				copy(hr.composite[8:], addr)
				binary.BigEndian.PutUint64(hr.composite[8+20:], FirstContractIncarnation)
				copy(hr.composite[8+20+8:], k)
				enc, err = hr.tx.GetOne(kv.PlainStateR, hr.composite)
				if err != nil {
					return nil, err
				}
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

func (hr *HistoryReaderInc) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
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
	if txKey != nil && stateTxNum >= hr.txNum {
		if enc, noState, err = hr.as.ReadAccountCodeNoState(addr, hr.txNum); err != nil {
			return nil, err
		}
	}
	if !noState {
		cHash := codeHash.Bytes()
		if txKey == nil {
			enc, err = hr.chainTx.GetOne(kv.Code, cHash)
			if err != nil {
				return nil, err
			}
		} else {
			if !hr.rs.Done(stateTxNum) {
				hr.readError = true
				hr.stateTxNum = stateTxNum
				return nil, &RequiredStateError{StateTxNum: stateTxNum}
			}
			enc = hr.rs.Get(kv.CodeR, cHash, nil, stateTxNum)
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
					return nil, err
				}
			}
		}
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x], noState=%t, txNum: %d\n", address, enc, noState, hr.txNum)
	}
	return enc, nil
}

func (hr *HistoryReaderInc) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
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
	if txKey != nil && stateTxNum >= hr.txNum {
		if size, noState, err = hr.as.ReadAccountCodeSizeNoState(addr, hr.txNum); err != nil {
			return 0, err
		}
	}
	if !noState {
		cHash := codeHash.Bytes()
		if txKey == nil {
			enc, err := hr.chainTx.GetOne(kv.Code, hr.composite)
			if err != nil {
				return 0, err
			}
			size = len(enc)
		} else {
			if !hr.rs.Done(stateTxNum) {
				hr.readError = true
				hr.stateTxNum = stateTxNum
				return 0, &RequiredStateError{StateTxNum: stateTxNum}
			}
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
	}
	if hr.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d], txNum: %d\n", address, size, hr.txNum)
	}
	return size, nil
}

func (hr *HistoryReaderInc) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

func (hr *HistoryReaderInc) ResetError() {
	hr.readError = false
}

func (hr *HistoryReaderInc) ReadError() (uint64, bool) {
	return hr.stateTxNum, hr.readError
}
