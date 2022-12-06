package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type StateReconWriterInc struct {
	ac        *libstate.Aggregator22Context
	rs        *ReconState
	txNum     uint64
	tx        kv.Tx
	chainTx   kv.Tx
	composite []byte
}

func NewStateReconWriterInc(ac *libstate.Aggregator22Context, rs *ReconState) *StateReconWriterInc {
	return &StateReconWriterInc{
		ac: ac,
		rs: rs,
	}
}

func (w *StateReconWriterInc) SetTxNum(txNum uint64) {
	w.txNum = txNum
}

func (w *StateReconWriterInc) SetTx(tx kv.Tx) {
	w.tx = tx
}

func (w *StateReconWriterInc) SetChainTx(chainTx kv.Tx) {
	w.chainTx = chainTx
}

var addr1 common.Address = common.HexToAddress("0x0000000000000000000000000000000000001000")

func (w *StateReconWriterInc) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addr := address.Bytes()
	if address == addr1 {
		fmt.Printf("account [%x] => (balance: %d, nonce: %d), txNum = %d\n", addr, &account.Balance, account.Nonce, w.txNum)
	}
	txKey, err := w.tx.GetOne(kv.XAccount, addr)
	if err != nil {
		return err
	}
	if txKey == nil {
		if address == addr1 {
			fmt.Printf("account [%x] txKey nil => (balance: %d, nonce: %d), txNum = %d\n", addr, &account.Balance, account.Nonce, w.txNum)
		}
		return nil
	}
	if stateTxNum := binary.BigEndian.Uint64(txKey); stateTxNum != w.txNum {
		if address == addr1 {
			fmt.Printf("account [%x] txKey => (balance: %d, nonce: %d), txNum = %d, stateTxNum = %d\n", addr, &account.Balance, account.Nonce, w.txNum, stateTxNum)
		}
		return nil
	}
	value := make([]byte, account.EncodingLengthForStorage())
	if account.Incarnation > 0 {
		account.Incarnation = FirstContractIncarnation
	}
	account.EncodeForStorage(value)
	w.rs.Put(kv.PlainStateR, addr, nil, value, w.txNum)
	if address == addr1 {
		fmt.Printf("account done [%x] => (balance: %d, nonce: %d), txNum = %d\n", addr, &account.Balance, account.Nonce, w.txNum)
	}
	return nil
}

func (w *StateReconWriterInc) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	addr, codeHashBytes := address.Bytes(), codeHash.Bytes()
	txKey, err := w.tx.GetOne(kv.XCode, addr)
	if err != nil {
		return err
	}
	if txKey == nil {
		return nil
	}
	if stateTxNum := binary.BigEndian.Uint64(txKey); stateTxNum != w.txNum {
		return nil
	}
	if len(code) > 0 {
		w.rs.Put(kv.CodeR, codeHashBytes, nil, common.CopyBytes(code), w.txNum)
		w.rs.Put(kv.PlainContractR, dbutils.PlainGenerateStoragePrefix(addr, FirstContractIncarnation), nil, codeHashBytes, w.txNum)
	} else {
		w.rs.Delete(kv.PlainContractD, dbutils.PlainGenerateStoragePrefix(addr, FirstContractIncarnation), nil, w.txNum)
	}
	return nil
}

func (w *StateReconWriterInc) DeleteAccount(address common.Address, original *accounts.Account) error {
	addr := address.Bytes()
	txKey, err := w.tx.GetOne(kv.XAccount, addr)
	if err != nil {
		return err
	}
	if txKey != nil {
		if stateTxNum := binary.BigEndian.Uint64(txKey); stateTxNum == w.txNum {
			//fmt.Printf("delete account [%x]=>{} txNum: %d\n", address, w.txNum)
			w.rs.Delete(kv.PlainStateD, addr, nil, w.txNum)
		}
	}
	// Iterate over storage of this contract and delete it too
	var c kv.Cursor
	if c, err = w.chainTx.Cursor(kv.PlainState); err != nil {
		return err
	}
	defer c.Close()
	var k []byte
	for k, _, err = c.Seek(addr); err == nil && bytes.HasPrefix(k, addr); k, _, err = c.Next() {
		//fmt.Printf("delete account storage [%x] [%x]=>{} txNum: %d\n", address, k[20+8:], w.txNum)
		if len(k) > 20 {
			w.rs.Delete(kv.PlainStateD, addr, common.CopyBytes(k[20+8:]), w.txNum)
		}
	}
	if err != nil {
		return err
	}
	// Delete all pending storage for this contract
	w.rs.RemoveAll(kv.PlainStateR, addr)
	w.rs.RemoveAll(kv.PlainStateD, addr)
	// Delete code
	txKey, err = w.tx.GetOne(kv.XCode, addr)
	if err != nil {
		return err
	}
	if txKey == nil {
		return nil
	}
	if stateTxNum := binary.BigEndian.Uint64(txKey); stateTxNum != w.txNum {
		return nil
	}
	w.rs.Delete(kv.PlainContractD, dbutils.PlainGenerateStoragePrefix(addr, FirstContractIncarnation), nil, w.txNum)
	return nil
}

func (w *StateReconWriterInc) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if cap(w.composite) < 20+32 {
		w.composite = make([]byte, 20+32)
	} else {
		w.composite = w.composite[:20+32]
	}
	addr, k := address.Bytes(), key.Bytes()

	copy(w.composite, addr)
	copy(w.composite[20:], k)
	txKey, err := w.tx.GetOne(kv.XStorage, w.composite)
	if err != nil {
		return err
	}
	if txKey == nil {
		return nil
	}
	if stateTxNum := binary.BigEndian.Uint64(txKey); stateTxNum != w.txNum {
		return nil
	}
	if value.IsZero() {
		w.rs.Delete(kv.PlainStateD, addr, k, w.txNum)
		//fmt.Printf("delete storage [%x] [%x] => [%x], txNum: %d\n", address, *key, value.Bytes(), w.txNum)
	} else {
		//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, value.Bytes(), w.txNum)
		w.rs.Put(kv.PlainStateR, addr, k, value.Bytes(), w.txNum)
	}
	return nil
}

func (w *StateReconWriterInc) CreateContract(address common.Address) error {
	return nil
}
