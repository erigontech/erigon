package state

import (
	"fmt"
	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var _ StateWriter = (*WriterV4)(nil)

type WriterV4 struct {
	tx    kv.TemporalPutDel
	trace bool
}

func NewWriterV4(tx kv.TemporalPutDel) *WriterV4 {
	return &WriterV4{
		tx:    tx,
		trace: false,
	}
}

func (cw *WriterV4) WriteChangeSets() error { return nil }
func (cw *WriterV4) WriteHistory() error    { return nil }

func (w *WriterV4) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash)
	}
	if original.Incarnation > account.Incarnation {
		if err := w.tx.DomainDel(kv.CodeDomain, address.Bytes(), nil, nil, 0); err != nil {
			return err
		}
		if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
			return err
		}
	}
	value := accounts.SerialiseV3(account)
	return w.tx.DomainPut(kv.AccountsDomain, address.Bytes(), nil, value, nil, 0)
}

func (w *WriterV4) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address.Bytes(), codeHash, len(code))
	}
	return w.tx.DomainPut(kv.CodeDomain, address.Bytes(), nil, code, nil, 0)
}

func (w *WriterV4) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del account: %x\n", address)
	}
	return w.tx.DomainDel(kv.AccountsDomain, address.Bytes(), nil, nil, 0)
}

func (w *WriterV4) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	if w.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, *key, value.Bytes())
	}
	return w.tx.DomainPut(kv.StorageDomain, address.Bytes(), key.Bytes(), value.Bytes(), nil, 0)
}

func (w *WriterV4) CreateContract(address libcommon.Address) (err error) {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}
	//seems don't need delete code here - tests starting fail
	//if err = sd.DomainDel(kv.CodeDomain, address[:], nil, nil); err != nil {
	//	return err
	//}
	return w.tx.DomainDelPrefix(kv.StorageDomain, address[:])
}
