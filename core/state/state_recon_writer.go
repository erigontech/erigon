package state

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type StateReconWriter struct {
	a     *libstate.Aggregator
	rwTx  kv.RwTx
	txNum uint64
}

func NewStateReconWriter(a *libstate.Aggregator) *StateReconWriter {
	return &StateReconWriter{
		a: a,
	}
}

func (w *StateReconWriter) SetTxNum(txNum uint64) {
	w.txNum = txNum
	w.a.SetTxNum(txNum)
}

func (w *StateReconWriter) SetTx(rwTx kv.RwTx) {
	w.rwTx = rwTx
}

func (w *StateReconWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	found, txNum := w.a.MaxAccountsTxNum(address.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change account [%x] txNum = %d\n", address, txNum)
		return nil
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash)
	return w.rwTx.Put(kv.PlainState, address[:], value)
}

func (w *StateReconWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	found, txNum := w.a.MaxCodeTxNum(address.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change code [%x] txNum = %d\n", address, txNum)
		return nil
	}
	if err := w.rwTx.Put(kv.Code, codeHash[:], code); err != nil {
		return err
	}
	if len(code) > 0 {
		//fmt.Printf("code [%x] => [%x] CodeHash: %x\n", address, code, codeHash)
		return w.rwTx.Put(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], FirstContractIncarnation), codeHash[:])
	}
	return nil
}

func (w *StateReconWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	return nil
}

func (w *StateReconWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	found, txNum := w.a.MaxStorageTxNum(address.Bytes(), key.Bytes())
	if !found {
		return nil
	}
	if txNum != w.txNum {
		//fmt.Printf("no change code [%x] [%x] txNum = %d\n", address, *key, txNum)
		return nil
	}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), FirstContractIncarnation, key.Bytes())
	v := value.Bytes()
	if len(v) != 0 {
		//fmt.Printf("storage [%x] [%x] => [%x]\n", address, *key, v)
		if err := w.rwTx.Put(kv.PlainState, compositeKey, v); err != nil {
			return err
		}
	}
	return nil
}

func (w *StateReconWriter) CreateContract(address common.Address) error {
	return nil
}
