// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	crand "crypto/rand"
	"math"
	"math/big"
	mrand "math/rand"
	"sync/atomic"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

// HeaderChain implements the basic block header chain logic that is shared by
// core.BlockChain and light.LightChain. It is not usable in itself, only as
// a part of either structure.
//
// HeaderChain is responsible for maintaining the header chain including the
// header query and updating.
//
// The components maintained by headerchain includes: (1) total difficult
// (2) header (3) block hash -> number mapping (4) canonical number -> hash mapping
// and (5) head header flag.
//
// It is not thread safe either, the encapsulating chain structures should do
// the necessary mutex locking/unlocking.
type HeaderChain struct {
	config *params.ChainConfig

	chainDb       ethdb.Database
	genesisHeader *types.Header

	currentHeader     atomic.Value // Current head of the header chain (may be above the block chain!)
	currentHeaderHash common.Hash  // Hash of the current head of the header chain (prevent recomputing all the time)

	procInterrupt func() bool

	rand   *mrand.Rand
	engine consensus.Engine
}

// NewHeaderChain creates a new HeaderChain structure. ProcInterrupt points
// to the parent's interrupt semaphore.
func NewHeaderChain(chainDb ethdb.Database, config *params.ChainConfig, engine consensus.Engine, procInterrupt func() bool) (*HeaderChain, error) {

	// Seed a fast but crypto originating random generator
	seed, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return nil, err
	}

	hc := &HeaderChain{
		config:        config,
		chainDb:       chainDb,
		procInterrupt: procInterrupt,
		rand:          mrand.New(mrand.NewSource(seed.Int64())),
		engine:        engine,
	}

	hc.genesisHeader = hc.GetHeaderByNumber(0)
	if hc.genesisHeader == nil {
		return nil, ErrNoGenesis
	}

	hc.currentHeader.Store(hc.genesisHeader)
	if head := rawdb.ReadHeadBlockHash(chainDb); head != (common.Hash{}) {
		if chead := hc.GetHeaderByHash(head); chead != nil {
			hc.currentHeader.Store(chead)
		}
	}
	hc.currentHeaderHash = hc.CurrentHeader().Hash()
	//headHeaderGauge.Update(hc.CurrentHeader().Number.Int64())

	return hc, nil
}

// GetBlockNumber retrieves the block number belonging to the given hash
// from the cache or database
func (hc *HeaderChain) GetBlockNumber(dbr ethdb.Database, hash common.Hash) *uint64 {
	number := rawdb.ReadHeaderNumber(dbr, hash)
	return number
}

// GetBlockHashesFromHash retrieves a number of block hashes starting at a given
// hash, fetching towards the genesis block.
func (hc *HeaderChain) GetBlockHashesFromHash(hash common.Hash, max uint64) []common.Hash {
	// Get the origin header from which to fetch
	header := hc.GetHeaderByHash(hash)
	if header == nil {
		return nil
	}
	// Iterate the headers until enough is collected or the genesis reached
	chain := make([]common.Hash, 0, max)
	for i := uint64(0); i < max; i++ {
		next := header.ParentHash
		if header = hc.GetHeader(next, header.Number.Uint64()-1); header == nil {
			break
		}
		chain = append(chain, next)
		if header.Number.Sign() == 0 {
			break
		}
	}
	return chain
}

// GetTd retrieves a block's total difficulty in the canonical chain from the
// database by hash and number, caching it if found.
func (hc *HeaderChain) GetTd(dbr ethdb.Getter, hash common.Hash, number uint64) *big.Int {
	td, _ := rawdb.ReadTd(dbr, hash, number)
	return td
}

// GetTdByHash retrieves a block's total difficulty in the canonical chain from the
// database by hash, caching it if found.
func (hc *HeaderChain) GetTdByHash(hash common.Hash) *big.Int {
	number := hc.GetBlockNumber(hc.chainDb, hash)
	if number == nil {
		return nil
	}
	return hc.GetTd(hc.chainDb, hash, *number)
}

// GetHeader retrieves a block header from the database by hash and number,
// caching it if found.
func (hc *HeaderChain) GetHeader(hash common.Hash, number uint64) *types.Header {
	header := rawdb.ReadHeader(hc.chainDb, hash, number)
	if header == nil {
		return nil
	}
	return header
}

// GetHeaderByHash retrieves a block header from the database by hash, caching it if
// found.
func (hc *HeaderChain) GetHeaderByHash(hash common.Hash) *types.Header {
	number := hc.GetBlockNumber(hc.chainDb, hash)
	if number == nil {
		return nil
	}
	return hc.GetHeader(hash, *number)
}

// HasHeader checks if a block header is present in the database or not.
// In theory, if header is present in the database, all relative components
// like td and hash->number should be present too.
func (hc *HeaderChain) HasHeader(hash common.Hash, number uint64) bool {
	return rawdb.HasHeader(hc.chainDb, hash, number)
}

// GetHeaderByNumber retrieves a block header from the database by number,
// caching it (associated with its hash) if found.
func (hc *HeaderChain) GetHeaderByNumber(number uint64) *types.Header {
	hash, err := rawdb.ReadCanonicalHash(hc.chainDb, number)
	if err != nil {
		panic(err)
	}

	if hash == (common.Hash{}) {
		return nil
	}
	return hc.GetHeader(hash, number)
}

func (hc *HeaderChain) GetCanonicalHash(number uint64) common.Hash {
	h, err := rawdb.ReadCanonicalHash(hc.chainDb, number)
	if err != nil {
		panic(err)
	}
	return h
}

// CurrentHeader retrieves the current head header of the canonical chain.
func (hc *HeaderChain) CurrentHeader() *types.Header {
	headHash := rawdb.ReadHeadHeaderHash(hc.chainDb)
	headNumber := rawdb.ReadHeaderNumber(hc.chainDb, headHash)
	return rawdb.ReadHeader(hc.chainDb, headHash, *headNumber)
}

// SetCurrentHeader sets the current head header of the canonical chain.
func (hc *HeaderChain) SetCurrentHeader(dbw ethdb.Putter, head *types.Header) {
	rawdb.WriteHeadHeaderHash(dbw, head.Hash())
	hc.currentHeader.Store(head)
	hc.currentHeaderHash = head.Hash()
	//headHeaderGauge.Update(head.Number.Int64())
}

type (
	// UpdateHeadBlocksCallback is a callback function that is called by SetHead
	// before head header is updated. The method will return the actual block it
	// updated the head to (missing state) and a flag if setHead should continue
	// rewinding till that forcefully (exceeded ancient limits)
	UpdateHeadBlocksCallback func(ethdb.Database, *types.Header) (uint64, bool)

	// DeleteBlockContentCallback is a callback function that is called by SetHead
	// before each header is deleted.
	DeleteBlockContentCallback func(ethdb.Database, common.Hash, uint64)
)

// SetHead rewinds the local chain to a new head. Everything above the new head
// will be deleted and the new one set.
func (hc *HeaderChain) SetHead(head uint64, updateFn UpdateHeadBlocksCallback, delFn DeleteBlockContentCallback) {
	var (
		parentHash common.Hash
		batch      = hc.chainDb.NewBatch()
	)
	for hdr := hc.CurrentHeader(); hdr != nil && hdr.Number.Uint64() > head; hdr = hc.CurrentHeader() {
		num := hdr.Number.Uint64()

		// Rewind block chain to new head.
		parent := hc.GetHeader(hdr.ParentHash, num-1)
		if parent == nil {
			parent = hc.genesisHeader
		}
		parentHash = hdr.ParentHash

		// Notably, since geth has the possibility for setting the head to a low
		// height which is even lower than ancient head.
		// In order to ensure that the head is always no higher than the data in
		// the database (ancient store or active store), we need to update head
		// first then remove the relative data from the database.
		//
		// Update head first(head fast block, head full block) before deleting the data.
		markerBatch := hc.chainDb.NewBatch()
		if updateFn != nil {
			newHead, force := updateFn(markerBatch, parent)
			if force && newHead < head {
				log.Warn("Force rewinding till ancient limit", "head", newHead)
				head = newHead
			}
		}
		// Update head header then.
		rawdb.WriteHeadHeaderHash(markerBatch, parentHash)
		if err := markerBatch.Commit(); err != nil {
			log.Crit("Failed to update chain markers", "error", err)
		}
		hc.currentHeader.Store(parent)
		hc.currentHeaderHash = parentHash
		//headHeaderGauge.Update(parent.Number.Int64())

		// Remove the related data from the database on all sidechains
		// Gather all the side fork hashes
		hash := hdr.Hash()
		if delFn != nil {
			delFn(batch, hash, num)
		}
		rawdb.DeleteHeader(batch, hash, num)
		if err := rawdb.DeleteTd(batch, hash, num); err != nil {
			panic(err)
		}

		if err := rawdb.DeleteCanonicalHash(batch, num); err != nil {
			panic(err)
		}
	}
	if err := batch.Commit(); err != nil {
		panic(err)
	}
}

// Config retrieves the header chain's chain configuration.
func (hc *HeaderChain) Config() *params.ChainConfig { return hc.config }

// Engine retrieves the header chain's consensus engine.
func (hc *HeaderChain) Engine() consensus.Engine { return hc.engine }

func (hc *HeaderChain) SetEngine(engine consensus.Engine) {
	hc.engine = engine
}

// GetBlock implements consensus.ChainReader, and returns nil for every input as
// a header chain does not have blocks available for retrieval.
func (hc *HeaderChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	return nil
}
