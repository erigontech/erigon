package whitelist

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type finality[T rawdb.BlockFinality[T]] struct {
	sync.RWMutex
	db       kv.RwDB
	Hash     common.Hash // Whitelisted Hash, populated by reaching out to heimdall
	Number   uint64      // Number , populated by reaching out to heimdall
	interval uint64      // Interval, until which we can allow importing
	doExist  bool
}

type finalityService interface {
	IsValidPeer(fetchHeadersByNumber func(number uint64, amount int, skip int, reverse bool) ([]*types.Header, []common.Hash, error)) (bool, error)
	IsValidChain(currentHeader *types.Header, chain []*types.Header) bool
	Get() (bool, uint64, common.Hash)
	Process(block uint64, hash common.Hash)
	Purge()
}

// IsValidPeer checks if the chain we're about to receive from a peer is valid or not
// in terms of reorgs. We won't reorg beyond the last bor finality submitted to mainchain.
func (f *finality[T]) IsValidPeer(fetchHeadersByNumber func(number uint64, amount int, skip int, reverse bool) ([]*types.Header, []common.Hash, error)) (bool, error) {
	// We want to validate the chain by comparing the last finalized block
	f.RLock()

	doExist := f.doExist
	number := f.Number
	hash := f.Hash

	f.RUnlock()

	return isValidPeer(fetchHeadersByNumber, doExist, number, hash)
}

// IsValidChain checks the validity of chain by comparing it
// against the local checkpoint entry
// todo: need changes
func (f *finality[T]) IsValidChain(currentHeader *types.Header, chain []*types.Header) bool {
	// Return if we've received empty chain
	if len(chain) == 0 {
		return false
	}

	res := isValidChain(currentHeader, chain, f.doExist, f.Number, f.Hash, f.interval)

	return res
}

func (f *finality[T]) Process(block uint64, hash common.Hash) {
	f.doExist = true
	f.Hash = hash
	f.Number = block

	err := rawdb.WriteLastFinality[T](f.db, block, hash)

	if err != nil {
		log.Error("Error in writing whitelist state to db", "err", err)
	}
}

// Get returns the existing whitelisted
// entries of checkpoint of the form (doExist,block number,block hash.)
func (f *finality[T]) Get() (bool, uint64, common.Hash) {
	f.RLock()
	defer f.RUnlock()

	if f.doExist {
		return f.doExist, f.Number, f.Hash
	}

	block, hash, err := rawdb.ReadFinality[T](f.db)
	if err != nil {
		return false, f.Number, f.Hash
	}

	return true, block, hash
}

// Purge purges the whitlisted checkpoint
func (f *finality[T]) Purge() {
	f.Lock()
	defer f.Unlock()

	f.doExist = false
}
