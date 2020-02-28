package state

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

// ChangeSetWriter is a mock StateWriter that accumulates changes in-memory into ChangeSets.
type ChangeSetWriter struct {
	accountChanges   map[common.Address][]byte
	accountNoChanges map[common.Address][]byte
}

func NewChangeSetWriter() *ChangeSetWriter {
	return &ChangeSetWriter{
		accountChanges:   make(map[common.Address][]byte),
		accountNoChanges: make(map[common.Address][]byte),
	}
}

func (w *ChangeSetWriter) GetAccountChanges(inclNoChanges bool) *dbutils.ChangeSet {
	cs := new(dbutils.ChangeSet)
	for key, val := range w.accountChanges {
		if err := cs.Add(crypto.Keccak256(key.Bytes()), val); err != nil {
			panic(err)
		}
	}
	if !inclNoChanges {
		return cs
	}
	for key, val := range w.accountNoChanges {
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
	if accountsEqual(original, account) {
		w.accountNoChanges[address] = accountData(original)
	} else {
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
	return nil
}

func (w *ChangeSetWriter) CreateContract(address common.Address) error {
	return nil
}
