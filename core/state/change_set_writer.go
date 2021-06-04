package state

import (
	"bytes"
	"context"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
)

// ChangeSetWriter is a mock StateWriter that accumulates changes in-memory into ChangeSets.
type ChangeSetWriter struct {
	db             ethdb.RwTx
	accountChanges map[common.Address][]byte
	storageChanged map[common.Address]bool
	storageChanges map[string][]byte
	blockNumber    uint64
}

func NewChangeSetWriter() *ChangeSetWriter {
	return &ChangeSetWriter{
		accountChanges: make(map[common.Address][]byte),
		storageChanged: make(map[common.Address]bool),
		storageChanges: make(map[string][]byte),
	}
}
func NewChangeSetWriterPlain(db ethdb.RwTx, blockNumber uint64) *ChangeSetWriter {
	return &ChangeSetWriter{
		db:             db,
		accountChanges: make(map[common.Address][]byte),
		storageChanged: make(map[common.Address]bool),
		storageChanges: make(map[string][]byte),
		blockNumber:    blockNumber,
	}
}

func (w *ChangeSetWriter) GetAccountChanges() (*changeset.ChangeSet, error) {
	cs := changeset.NewAccountChangeSet()
	for address, val := range w.accountChanges {
		if err := cs.Add(common.CopyBytes(address[:]), val); err != nil {
			return nil, err
		}
	}
	return cs, nil
}
func (w *ChangeSetWriter) GetStorageChanges() (*changeset.ChangeSet, error) {
	cs := changeset.NewStorageChangeSet()
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

func (w *ChangeSetWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if !accountsEqual(original, account) || w.storageChanged[address] {
		w.accountChanges[address] = originalAccountData(original, true /*omitHashes*/)
	}
	return nil
}

func (w *ChangeSetWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (w *ChangeSetWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	w.accountChanges[address] = originalAccountData(original, false)
	return nil
}

func (w *ChangeSetWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}

	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())

	w.storageChanges[string(compositeKey)] = original.Bytes()
	w.storageChanged[address] = true

	return nil
}

func (w *ChangeSetWriter) CreateContract(address common.Address) error {
	return nil
}

func (w *ChangeSetWriter) WriteChangeSets() error {
	accountChanges, err := w.GetAccountChanges()
	if err != nil {
		return err
	}
	var prevK []byte
	if err = changeset.Mapper[dbutils.AccountChangeSetBucket].Encode(w.blockNumber, accountChanges, func(k, v []byte) error {
		if bytes.Equal(k, prevK) {
			if err = w.db.AppendDup(dbutils.AccountChangeSetBucket, k, v); err != nil {
				return err
			}
		} else {
			if err = w.db.Append(dbutils.AccountChangeSetBucket, k, v); err != nil {
				return err
			}
		}
		prevK = k
		return nil
	}); err != nil {
		return err
	}
	prevK = nil

	storageChanges, err := w.GetStorageChanges()
	if err != nil {
		return err
	}
	if storageChanges.Len() == 0 {
		return nil
	}
	if err = changeset.Mapper[dbutils.StorageChangeSetBucket].Encode(w.blockNumber, storageChanges, func(k, v []byte) error {
		if bytes.Equal(k, prevK) {
			if err = w.db.AppendDup(dbutils.StorageChangeSetBucket, k, v); err != nil {
				return err
			}
		} else {
			if err = w.db.Append(dbutils.StorageChangeSetBucket, k, v); err != nil {
				return err
			}
		}
		prevK = k
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
	err = writeIndex(w.blockNumber, accountChanges, dbutils.AccountsHistoryBucket, w.db)
	if err != nil {
		return err
	}

	storageChanges, err := w.GetStorageChanges()
	if err != nil {
		return err
	}
	err = writeIndex(w.blockNumber, storageChanges, dbutils.StorageHistoryBucket, w.db)
	if err != nil {
		return err
	}

	return nil
}

func (w *ChangeSetWriter) PrintChangedAccounts() {
	fmt.Println("Account Changes")
	for k := range w.accountChanges {
		fmt.Println(hexutil.Encode(k.Bytes()))
	}
	fmt.Println("------------------------------------------")
}
