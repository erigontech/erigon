package state

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

// FakeDB is the implementation of the fakeevm.FakeDB interface
type FakeDB struct {
	State     *State
	stateRoot []byte
}

// SetStateRoot is the stateRoot setter.
func (f *FakeDB) SetStateRoot(stateRoot []byte) {
	f.stateRoot = stateRoot
}

// GetBalance returns the balance of the given address.
func (f *FakeDB) GetBalance(address common.Address) *big.Int {
	ctx := context.Background()
	balance, err := f.State.GetTree().GetBalance(ctx, address, f.stateRoot)

	if err != nil {
		log.Errorf("error on FakeDB GetBalance for address %v", address)
	}

	log.Debugf("FakeDB GetBalance for address %v", address)
	return balance
}

// GetNonce returns the nonce of the given address.
func (f *FakeDB) GetNonce(address common.Address) uint64 {
	ctx := context.Background()
	nonce, err := f.State.GetTree().GetNonce(ctx, address, f.stateRoot)

	if err != nil {
		log.Errorf("error on FakeDB GetNonce for address %v", address)
		return 0
	}

	log.Debugf("FakeDB GetNonce for address %v", address)
	return nonce.Uint64()
}

// GetCode returns the SC code of the given address.
func (f *FakeDB) GetCode(address common.Address) []byte {
	ctx := context.Background()
	code, err := f.State.GetTree().GetCode(ctx, address, f.stateRoot)

	if err != nil {
		log.Errorf("error on FakeDB GetCode for address %v", address)
	}

	log.Debugf("FakeDB GetCode for address %v", address)
	return code
}

// GetState retrieves a value from the given account's storage trie.
func (f *FakeDB) GetState(address common.Address, hash common.Hash) common.Hash {
	ctx := context.Background()
	storage, err := f.State.GetTree().GetStorageAt(ctx, address, hash.Big(), f.stateRoot)

	if err != nil {
		log.Errorf("error on FakeDB GetState for address %v", address)
	}

	log.Debugf("FakeDB GetState for address %v", address)

	return common.BytesToHash(storage.Bytes())
}

// Exist determines if the given address is in use.
func (f *FakeDB) Exist(address common.Address) bool {
	return !(f.GetNonce(address) == 0 && f.GetBalance(address).Int64() == 0 && f.GetCodeHash(address) == ZeroHash)
}

// GetCodeHash gets the hash for the code at a given address
func (f *FakeDB) GetCodeHash(address common.Address) common.Hash {
	ctx := context.Background()
	hash, err := f.State.GetTree().GetCodeHash(ctx, address, f.stateRoot)

	if err != nil {
		log.Errorf("error on FakeDB GetCodeHash for address %v, err: %v", address, err)
	}

	log.Debugf("FakeDB GetCodeHash for address %v => %v", address, common.BytesToHash(hash))
	return common.BytesToHash(hash)
}
