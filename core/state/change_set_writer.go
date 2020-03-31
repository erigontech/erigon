package state

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

// ChangeSetWriter is a mock StateWriter that accumulates changes in-memory into ChangeSets.
type ChangeSetWriter struct {
	accountChanges map[common.Address][]byte
	storageChanged map[common.Address]bool
	storageChanges map[string][]byte
}

func NewChangeSetWriter() *ChangeSetWriter {
	return &ChangeSetWriter{
		accountChanges: make(map[common.Address][]byte),
		storageChanged: make(map[common.Address]bool),
		storageChanges: make(map[string][]byte),
	}
}

func (w *ChangeSetWriter) GetAccountChanges() *changeset.ChangeSet {
	cs := changeset.NewAccountChangeSet()
	for key, val := range w.accountChanges {
		if err := cs.Add(crypto.Keccak256(key.Bytes()), val); err != nil {
			panic(err)
		}
	}
	return cs
}

func (w *ChangeSetWriter) GetStorageChanges() *changeset.ChangeSet {
	cs := changeset.NewStorageChangeSet()
	for key, val := range w.storageChanges {
		if err := cs.Add([]byte(key), val); err != nil {
			panic(err)
		}
	}
	return cs
}

func (w *ChangeSetWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if !accountsEqual(original, account) || w.storageChanged[address] {
		w.accountChanges[address] = originalAccountData(original, debug.IsThinHistory())
	}
	return nil
}

func (w *ChangeSetWriter) UpdateAccountCode(addrHash common.Hash, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (w *ChangeSetWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	w.accountChanges[address] = originalAccountData(original, false)
	return nil
}

func (w *ChangeSetWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	if *original == *value {
		return nil
	}

	secKey, err := common.HashData(key[:])
	if err != nil {
		return err
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}

	if incarnation == 0 {
		incarnation = FirstContractIncarnation
	}

	compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, secKey)

	o := bytes.TrimLeft(original[:], "\x00")
	originalValue := make([]byte, len(o))
	copy(originalValue, o)

	w.storageChanges[string(compositeKey)] = originalValue
	w.storageChanged[address] = true

	return nil
}

func (w *ChangeSetWriter) CreateContract(address common.Address) error {
	return nil
}

func (w *ChangeSetWriter) PrintChangedAccounts() {
	fmt.Println("Account Changes")
	for k := range w.accountChanges {
		fmt.Println(hexutil.Encode(k.Bytes()))
	}
	fmt.Println("------------------------------------------")
}
