package state

import (
	"bytes"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type StateReconWriterInc struct {
	as      *libstate.AggregatorStep
	rs      *ReconState
	txNum   uint64
	tx      kv.Tx
	chainTx kv.Tx
}

func NewStateReconWriterInc(as *libstate.AggregatorStep, rs *ReconState) *StateReconWriterInc {
	return &StateReconWriterInc{
		as: as,
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

func (w *StateReconWriterInc) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	addr := address.Bytes()
	if ok, stateTxNum := w.as.MaxTxNumAccounts(addr); !ok || stateTxNum != w.txNum {
		return nil
	}
	value := make([]byte, account.EncodingLengthForStorage())
	if account.Incarnation > 0 {
		account.Incarnation = FirstContractIncarnation
	}
	account.EncodeForStorage(value)
	w.rs.Put(kv.PlainStateR, addr, nil, value, w.txNum)
	return nil
}

func (w *StateReconWriterInc) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	addr, codeHashBytes := address.Bytes(), codeHash.Bytes()
	if ok, stateTxNum := w.as.MaxTxNumCode(addr); !ok || stateTxNum != w.txNum {
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

func (w *StateReconWriterInc) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	addr := address.Bytes()
	if ok, stateTxNum := w.as.MaxTxNumAccounts(addr); ok && stateTxNum == w.txNum {
		//fmt.Printf("delete account [%x]=>{} txNum: %d\n", address, w.txNum)
		w.rs.Delete(kv.PlainStateD, addr, nil, w.txNum)
	}
	// Iterate over storage of this contract and delete it too
	var c kv.Cursor
	var err error
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
	if ok, stateTxNum := w.as.MaxTxNumCode(addr); ok && stateTxNum == w.txNum {
		w.rs.Delete(kv.PlainContractD, dbutils.PlainGenerateStoragePrefix(addr, FirstContractIncarnation), nil, w.txNum)
	}
	return nil
}

func (w *StateReconWriterInc) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	addr, k := address.Bytes(), key.Bytes()
	if ok, stateTxNum := w.as.MaxTxNumStorage(addr, k); !ok || stateTxNum != w.txNum {
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

func (w *StateReconWriterInc) CreateContract(address libcommon.Address) error {
	return nil
}
