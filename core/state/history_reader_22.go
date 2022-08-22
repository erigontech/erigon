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

// Implements StateReader and StateWriter
type HistoryReader22 struct {
	ac    *libstate.AggregatorContext
	ri    *libstate.ReadIndices
	txNum uint64
	trace bool
}

func NewHistoryReader22(ac *libstate.AggregatorContext, ri *libstate.ReadIndices) *HistoryReader22 {
	return &HistoryReader22{ac: ac, ri: ri}
}

func (hr *HistoryReader22) SetTx(tx kv.RwTx) {
	hr.ri.SetTx(tx)
}

func (hr *HistoryReader22) SetTxNum(txNum uint64) {
	hr.txNum = txNum
	if hr.ri != nil {
		hr.ri.SetTxNum(txNum)
	}
}

func (hr *HistoryReader22) FinishTx() error {
	return hr.ri.FinishTx()
}

func (hr *HistoryReader22) SetTrace(trace bool) {
	hr.trace = trace
}

func (hr *HistoryReader22) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if hr.ri != nil {
		if err := hr.ri.ReadAccountData(address.Bytes()); err != nil {
			return nil, err
		}
	}
	enc, err := hr.ac.ReadAccountDataBeforeTxNum(address.Bytes(), hr.txNum, nil /* roTx */)
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
	if err := deserialise2(&a, enc); err != nil {
		return nil, fmt.Errorf("ReadAccountData(%x): %w", address, err)
	}

	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
	}
	return &a, nil
}

func deserialise2(a *accounts.Account, enc []byte) error {
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
	if pos >= len(enc) {
		return fmt.Errorf("deserialse2: %d >= %d ", pos, len(enc))
	}
	incBytes := int(enc[pos])
	pos++
	if incBytes > 0 {
		a.Incarnation = bytesToUint64(enc[pos : pos+incBytes])
	}
	return nil
}

func (hr *HistoryReader22) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	if hr.ri != nil {
		if err := hr.ri.ReadAccountStorage(address.Bytes(), key.Bytes()); err != nil {
			return nil, err
		}
	}
	enc, err := hr.ac.ReadAccountStorageBeforeTxNum(address.Bytes(), key.Bytes(), hr.txNum, nil /* roTx */)
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

func (hr *HistoryReader22) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
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

func (hr *HistoryReader22) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
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

func (hr *HistoryReader22) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}
