package state

import (
	"bytes"
	"context"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type changesetFactory func() *changeset.ChangeSet
type accountKeyGen func(common.Address) ([]byte, error)
type storageKeyGen func(common.Address, uint64, common.Hash) ([]byte, error)

func plainAccountKeyGen(a common.Address) ([]byte, error) {
	return a[:], nil
}

func hashedAccountKeyGen(a common.Address) ([]byte, error) {
	addrHash, err := common.HashData(a[:])
	return addrHash[:], err
}

func hashedStorageKeyGen(address common.Address, incarnation uint64, key common.Hash) ([]byte, error) {
	secKey, err := common.HashData(key[:])
	if err != nil {
		return nil, err
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	return dbutils.GenerateCompositeStorageKey(addrHash, incarnation, secKey), nil
}

func plainStorageKeyGen(address common.Address, incarnation uint64, key common.Hash) ([]byte, error) {
	return dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes()), nil
}

// ChangeSetWriter is a mock StateWriter that accumulates changes in-memory into ChangeSets.
type ChangeSetWriter struct {
	db             ethdb.Database
	accountChanges map[common.Address][]byte
	storageChanged map[common.Address]bool
	storageChanges map[string][]byte
	storageFactory changesetFactory
	accountFactory changesetFactory
	accountKeyGen  accountKeyGen
	storageKeyGen  storageKeyGen
	blockNumber    uint64
}

func NewChangeSetWriterPlain(db ethdb.Database, blockNumber uint64) *ChangeSetWriter {
	return &ChangeSetWriter{
		db:             db,
		accountChanges: make(map[common.Address][]byte),
		storageChanged: make(map[common.Address]bool),
		storageChanges: make(map[string][]byte),
		storageFactory: changeset.NewStorageChangeSetPlain,
		accountFactory: changeset.NewAccountChangeSetPlain,
		accountKeyGen:  plainAccountKeyGen,
		storageKeyGen:  plainStorageKeyGen,
		blockNumber:    blockNumber,
	}
}

func (w *ChangeSetWriter) GetAccountChanges() (*changeset.ChangeSet, error) {
	cs := w.accountFactory()
	for address, val := range w.accountChanges {
		key, err := w.accountKeyGen(address)
		if err != nil {
			return nil, err
		}
		if err := cs.Add(key, val); err != nil {
			return nil, err
		}
	}
	return cs, nil
}
func (w *ChangeSetWriter) GetStorageChanges() (*changeset.ChangeSet, error) {
	cs := w.storageFactory()
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

	compositeKey, err := w.storageKeyGen(address, incarnation, *key)
	if err != nil {
		return err
	}

	w.storageChanges[string(compositeKey)] = original.Bytes()
	w.storageChanged[address] = true

	return nil
}

func (w *ChangeSetWriter) CreateContract(address common.Address) error {
	return nil
}

func (w *ChangeSetWriter) WriteChangeSets() error {
	db := w.db
	accountChanges, err := w.GetAccountChanges()
	if err != nil {
		return err
	}
	var prevK []byte
	if err = changeset.Mapper[dbutils.PlainAccountChangeSetBucket].Encode(w.blockNumber, accountChanges, func(k, v []byte) error {
		if bytes.Equal(k, prevK) {
			if err = db.AppendDup(dbutils.PlainAccountChangeSetBucket, k, v); err != nil {
				return err
			}
		} else {
			if err = db.Append(dbutils.PlainAccountChangeSetBucket, k, v); err != nil {
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
	if err = changeset.Mapper[dbutils.PlainStorageChangeSetBucket].Encode(w.blockNumber, storageChanges, func(k, v []byte) error {
		if bytes.Equal(k, prevK) {
			if err = db.AppendDup(dbutils.PlainStorageChangeSetBucket, k, v); err != nil {
				return err
			}
		} else {
			if err = db.Append(dbutils.PlainStorageChangeSetBucket, k, v); err != nil {
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
