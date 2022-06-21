package state

import (
	"encoding/binary"

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
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	return w.rwTx.Put(kv.PlainState, address[:], value)
}

func (w *StateReconWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := w.rwTx.Put(kv.Code, codeHash[:], code); err != nil {
		return err
	}
	return w.rwTx.Put(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], incarnation), codeHash[:])
}

func (w *StateReconWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	if err := w.rwTx.Delete(kv.PlainState, address[:], nil); err != nil {
		return err
	}
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		if err := w.rwTx.Put(kv.IncarnationMap, address[:], b[:]); err != nil {
			return err
		}
	}
	return nil
}

func (w *StateReconWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	v := value.Bytes()
	if len(v) == 0 {
		return w.rwTx.Delete(kv.PlainState, compositeKey, nil)
	}
	return w.rwTx.Put(kv.PlainState, compositeKey, v)
}

func (w *StateReconWriter) CreateContract(address common.Address) error {
	return nil
}
