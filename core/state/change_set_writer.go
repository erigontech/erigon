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
	accountChanges map[common.Address][]byte
}

func NewChangeSetWriter() *ChangeSetWriter {
	return &ChangeSetWriter{accountChanges: make(map[common.Address][]byte)}
}

func (w *ChangeSetWriter) GetAccountChanges() *dbutils.ChangeSet {
	cs := new(dbutils.ChangeSet)
	for key, val := range w.accountChanges {
		cs.Add(crypto.Keccak256(key.Bytes()), val)
	}
	return cs
}

func (w *ChangeSetWriter) addAccountChange(address common.Address, original *accounts.Account) {
	var originalData []byte
	if !original.Initialised {
		originalData = []byte{}
	} else {
		originalDataLen := original.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		original.EncodeForStorage(originalData)
	}

	w.accountChanges[address] = originalData
}

func (w *ChangeSetWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	w.addAccountChange(address, original)
	return nil
}

func (w *ChangeSetWriter) UpdateAccountCode(addrHash common.Hash, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (w *ChangeSetWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	w.addAccountChange(address, original)
	return nil
}

func (w *ChangeSetWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	return nil
}

func (w *ChangeSetWriter) CreateContract(address common.Address) error {
	return nil
}
