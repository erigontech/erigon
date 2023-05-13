package state

import (
	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/state/temporal"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var _ StateWriter = (*WriterV4)(nil)

type WriterV4 struct {
	tx  kv.TemporalTx
	agg *state.AggregatorV3
}

func NewWriterV4(tx kv.TemporalTx) *WriterV4 {
	return &WriterV4{tx: tx, agg: tx.(*temporal.Tx).Agg()}
}

func (w *WriterV4) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	value := accounts.SerialiseV3(account)
	origValue := accounts.SerialiseV3(original)
	w.agg.SetTx(w.tx.(kv.RwTx))
	//fmt.Printf("v4 account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash)
	return w.agg.UpdateAccount(address.Bytes(), value, origValue)
}

func (w *WriterV4) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	w.agg.SetTx(w.tx.(kv.RwTx))
	return w.agg.UpdateCode(address.Bytes(), code, nil)
}

func (w *WriterV4) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	w.agg.SetTx(w.tx.(kv.RwTx))
	prev := accounts.SerialiseV3(original)

	return w.agg.DeleteAccount(address.Bytes(), prev)
}

func (w *WriterV4) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	w.agg.SetTx(w.tx.(kv.RwTx))
	return w.agg.UpdateStorage(address.Bytes(), key.Bytes(), value.Bytes(), original.Bytes())
}

func (w *WriterV4) CreateContract(address libcommon.Address) error { return nil }
func (w *WriterV4) WriteChangeSets() error                         { return nil }
func (w *WriterV4) WriteHistory() error                            { return nil }

func (w *WriterV4) Commitment(saveStateAfter, trace bool) (rootHash []byte, err error) {
	w.agg.SetTx(w.tx.(kv.RwTx))

	rh, err := w.agg.ComputeCommitment(saveStateAfter, trace)
	if err != nil {
		return nil, err
	}
	return rh, nil
}
