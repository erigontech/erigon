package state

import (
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var _ StateWriter = (*WriterV4)(nil)

type WriterV4 struct {
	tx kv.TemporalPutDel
}

func NewWriterV4(tx kv.TemporalPutDel) *WriterV4 {
	return &WriterV4{tx: tx}
}

func (w *WriterV4) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	if original.Incarnation > account.Incarnation {
		if err := w.tx.DomainDel(kv.CodeDomain, address.Bytes(), nil, nil); err != nil {
			return err
		}
		if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
			return err
		}
	}
	value, origValue := accounts.SerialiseV3(account), accounts.SerialiseV3(original)
	return w.tx.DomainPut(kv.AccountsDomain, address.Bytes(), nil, value, origValue)
}

func (w *WriterV4) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	return w.tx.DomainPut(kv.CodeDomain, address.Bytes(), nil, code, nil)
}

func (w *WriterV4) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	return w.tx.DomainPut(kv.AccountsDomain, address.Bytes(), nil, nil, nil)
}

func (w *WriterV4) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	return w.tx.DomainPut(kv.StorageDomain, address.Bytes(), key.Bytes(), value.Bytes(), original.Bytes())
}

func (w *WriterV4) CreateContract(address libcommon.Address) (err error) {
	//sd := w.tx.(*state.SharedDomains)
	//if err = sd.DomainDel(kv.CodeDomain, address[:], nil, nil); err != nil {
	//	return err
	//}
	sd := w.tx.(*state.SharedDomains)
	return sd.IterateStoragePrefix(address[:], func(k, v []byte) error {
		return w.tx.DomainPut(kv.StorageDomain, k, nil, nil, v)
	})
	//if err = w.tx.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
	//	return err
	//}
	return nil
}
func (w *WriterV4) WriteChangeSets() error { return nil }
func (w *WriterV4) WriteHistory() error    { return nil }
