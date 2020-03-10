package state

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

// ChangeSetWriter is a mock StateWriter that accumulates changes in-memory into ChangeSets.
type ChangeSetWriter struct {
	accountChanges map[common.Address][]byte
	storageChanges map[common.Address]bool
}

func NewChangeSetWriter() *ChangeSetWriter {
	return &ChangeSetWriter{
		accountChanges: make(map[common.Address][]byte),
		storageChanges: make(map[common.Address]bool),
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

func accountData(original *accounts.Account) []byte {
	var originalData []byte
	if !original.Initialised {
		originalData = []byte{}
	} else {
		originalDataLen := original.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		original.EncodeForStorage(originalData)
	}
	return originalData
}

func (w *ChangeSetWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if !accountsEqual(original, account) || w.storageChanges[address] {
		w.accountChanges[address] = accountData(original)
	}
	return nil
}

func (w *ChangeSetWriter) UpdateAccountCode(addrHash common.Hash, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (w *ChangeSetWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	w.accountChanges[address] = accountData(original)
	return nil
}

func (w *ChangeSetWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	if *original == *value {
		return nil
	}
	w.storageChanges[address] = true
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
	fmt.Println("Storage Changes")
	for k := range w.storageChanges {
		fmt.Println(hexutil.Encode(k.Bytes()))
	}
	fmt.Println("------------------------------------------")
}
