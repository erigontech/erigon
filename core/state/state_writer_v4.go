package state

import (
	"fmt"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/state/temporal"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var _ StateWriter = (*WriterV4)(nil)

type WriterV4 struct {
	tx kv.TemporalTx
}

func NewWriterV4(tx kv.TemporalTx) *WriterV4 {
	return &WriterV4{tx: tx}
}
func (w *WriterV4) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	value := accounts.SerialiseV3(account)
	origValue := accounts.SerialiseV3(original)
	fmt.Printf("write: %x, %d\n", address, account.Balance.Uint64())
	agg := w.tx.(*temporal.Tx).AggCtx()
	return agg.PutAccount(address.Bytes(), origValue, value, w.tx.(kv.RwTx))
}

func (w *WriterV4) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	agg := w.tx.(*temporal.Tx).AggCtx()
	return agg.PutCode(address.Bytes(), code, w.tx.(kv.RwTx))
}

func (w *WriterV4) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	agg := w.tx.(*temporal.Tx).AggCtx()
	fmt.Printf("del: %x\n", address)
	return agg.DeleteAccount(address.Bytes(), w.tx.(kv.RwTx))
}

func (w *WriterV4) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	agg := w.tx.(*temporal.Tx).AggCtx()
	return agg.PutStorage(address.Bytes(), key.Bytes(), original.Bytes(), value.Bytes(), w.tx.(kv.RwTx))
}

func (w *WriterV4) CreateContract(address libcommon.Address) error {
	return nil
}
