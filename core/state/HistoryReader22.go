package state

import (
	"fmt"

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
	a     *libstate.Aggregator
	txNum uint64
	trace bool
}

func NewHistoryReader22(a *libstate.Aggregator) *HistoryReader22 {
	return &HistoryReader22{a: a}
}

func (hr *HistoryReader22) SetTxNum(txNum uint64) {
	hr.txNum = txNum
	hr.a.SetTxNum(txNum)
}

func (hr *HistoryReader22) SetTrace(trace bool) {
	hr.trace = trace
}

func (hr *HistoryReader22) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := hr.a.ReadAccountDataBeforeTxNum(address.Bytes(), hr.txNum, nil /* roTx */)
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

func (hr *HistoryReader22) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	enc, err := hr.a.ReadAccountStorageBeforeTxNum(address.Bytes(), key.Bytes(), hr.txNum, nil /* roTx */)
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
	enc, err := hr.a.ReadAccountCodeBeforeTxNum(address.Bytes(), hr.txNum, nil /* roTx */)
	if err != nil {
		return nil, err
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x]\n", address, enc)
	}
	return enc, nil
}

func (hr *HistoryReader22) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	size, err := hr.a.ReadAccountCodeSizeBeforeTxNum(address.Bytes(), hr.txNum, nil /* roTx */)
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
