package state

import (
	"fmt"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	historyv22 "github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

// ChangeSetWriter is a mock StateWriter that accumulates changes in-memory into ChangeSets.
type ChangeSetWriter struct {
	db             kv.RwTx
	accountChanges map[libcommon.Address][]byte
	storageChanged map[libcommon.Address]bool
	storageChanges map[string][]byte
	blockNumber    uint64
}

func NewChangeSetWriter() *ChangeSetWriter {
	return &ChangeSetWriter{
		accountChanges: make(map[libcommon.Address][]byte),
		storageChanged: make(map[libcommon.Address]bool),
		storageChanges: make(map[string][]byte),
	}
}
func NewChangeSetWriterPlain(db kv.RwTx, blockNumber uint64) *ChangeSetWriter {
	return &ChangeSetWriter{
		db:             db,
		accountChanges: make(map[libcommon.Address][]byte),
		storageChanged: make(map[libcommon.Address]bool),
		storageChanges: make(map[string][]byte),
		blockNumber:    blockNumber,
	}
}

func (w *ChangeSetWriter) GetAccountChanges() (*historyv22.ChangeSet, error) {
	cs := historyv22.NewAccountChangeSet()
	for address, val := range w.accountChanges {
		if err := cs.Add(common.CopyBytes(address[:]), val); err != nil {
			return nil, err
		}
	}
	return cs, nil
}
func (w *ChangeSetWriter) GetStorageChanges() (*historyv22.ChangeSet, error) {
	cs := historyv22.NewStorageChangeSet()
	for key, val := range w.storageChanges {
		if err := cs.Add([]byte(key), val); err != nil {
			return nil, err
		}
	}
	return cs, nil
}

func accountsEqual(a1, a2 *accounts.Account) bool {
	if a1.Nonce != a2.Nonce {
		return false
	}
	if !a1.Initialised {
		if a2.Initialised {
			return false
		}
	} else if !a2.Initialised {
		return false
	} else if a1.Balance.Cmp(&a2.Balance) != 0 {
		return false
	}
	if a1.IsEmptyCodeHash() {
		if !a2.IsEmptyCodeHash() {
			return false
		}
	} else if a2.IsEmptyCodeHash() {
		return false
	} else if a1.CodeHash != a2.CodeHash {
		return false
	}
	return true
}

func (w *ChangeSetWriter) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	//fmt.Printf("balance,%x,%d\n", address, &account.Balance)
	if !accountsEqual(original, account) || w.storageChanged[address] {
		w.accountChanges[address] = originalAccountData(original, true /*omitHashes*/)
	}
	return nil
}

func (w *ChangeSetWriter) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	//fmt.Printf("code,%x,%x\n", address, code)
	return nil
}

func (w *ChangeSetWriter) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	//fmt.Printf("delete,%x\n", address)
	if original == nil || !original.Initialised {
		return nil
	}
	w.accountChanges[address] = originalAccountData(original, false)
	return nil
}

func (w *ChangeSetWriter) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	//fmt.Printf("storage,%x,%x,%x\n", address, *key, value.Bytes())
	if *original == *value {
		return nil
	}

	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())

	w.storageChanges[string(compositeKey)] = original.Bytes()
	w.storageChanged[address] = true

	return nil
}

func (w *ChangeSetWriter) CreateContract(address libcommon.Address) error {
	return nil
}

func (w *ChangeSetWriter) WriteChangeSets() error {
	accountChanges, err := w.GetAccountChanges()
	if err != nil {
		return err
	}
	if err = historyv22.Mapper[kv.AccountChangeSet].Encode(w.blockNumber, accountChanges, func(k, v []byte) error {
		if err = w.db.AppendDup(kv.AccountChangeSet, k, v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	storageChanges, err := w.GetStorageChanges()
	if err != nil {
		return err
	}
	if storageChanges.Len() == 0 {
		return nil
	}
	if err = historyv22.Mapper[kv.StorageChangeSet].Encode(w.blockNumber, storageChanges, func(k, v []byte) error {
		if err = w.db.AppendDup(kv.StorageChangeSet, k, v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (w *ChangeSetWriter) WriteHistory() error {
	accountChanges, err := w.GetAccountChanges()
	if err != nil {
		return err
	}
	err = writeIndex(w.blockNumber, accountChanges, kv.E2AccountsHistory, w.db)
	if err != nil {
		return err
	}

	storageChanges, err := w.GetStorageChanges()
	if err != nil {
		return err
	}
	err = writeIndex(w.blockNumber, storageChanges, kv.E2StorageHistory, w.db)
	if err != nil {
		return err
	}

	return nil
}

func (w *ChangeSetWriter) PrintChangedAccounts() {
	fmt.Println("Account Changes")
	for k := range w.accountChanges {
		fmt.Println(hexutility.Encode(k.Bytes()))
	}
	fmt.Println("------------------------------------------")
}
