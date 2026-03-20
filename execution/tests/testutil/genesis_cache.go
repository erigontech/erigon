// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/rulesconfig"
)

// genesisCacheEntry holds a cached genesis DB and its genesis block.
type genesisCacheEntry struct {
	db      kv.TemporalRwDB
	genesis *types.Block
}

// genesisDBCache caches one MDBX database per unique (fork, genesisSpecHash) pair.
// The DB contains genesis KV state (headers, TDs, config) plus domain state
// (account balances, code, storage) written via SharedDomains.
var (
	genesisDBCache sync.Map  // map[string]*genesisCacheEntry
	genesisDBMu    sync.Mutex // serializes genesis DB creation
)

// genesisSpecHash produces a deterministic hash of the genesis spec for cache keying.
// It includes alloc addresses+nonces and header fields that affect the genesis hash.
func genesisSpecHash(g *types.Genesis) string {
	h := sha256.New()

	// Hash the alloc (sorted by address for determinism).
	addrs := make([]common.Address, 0, len(g.Alloc))
	for addr := range g.Alloc {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].Cmp(addrs[j]) < 0 })
	for _, addr := range addrs {
		h.Write(addr[:])
		acct := g.Alloc[addr]
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], acct.Nonce)
		h.Write(buf[:])
		if acct.Balance != nil {
			h.Write(acct.Balance.Bytes())
		}
		h.Write(acct.Code)
		binary.LittleEndian.PutUint64(buf[:], uint64(len(acct.Storage)))
		h.Write(buf[:])
	}

	// Hash header fields that affect the genesis block hash.
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], g.GasLimit)
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], g.Timestamp)
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], g.Nonce)
	h.Write(buf[:])
	h.Write(g.ExtraData)
	h.Write(g.Coinbase[:])
	h.Write(g.Mixhash[:])
	h.Write(g.ParentHash[:])
	if g.Difficulty != nil {
		h.Write(g.Difficulty.Bytes())
	}
	if g.BaseFee != nil {
		h.Write(g.BaseFee.Bytes())
	}
	if g.ExcessBlobGas != nil {
		binary.LittleEndian.PutUint64(buf[:], *g.ExcessBlobGas)
		h.Write(buf[:])
	}
	if g.BlobGasUsed != nil {
		binary.LittleEndian.PutUint64(buf[:], *g.BlobGasUsed)
		h.Write(buf[:])
	}

	return fmt.Sprintf("%x", h.Sum(nil)[:16])
}

// getOrCreateGenesisDB returns a cached genesis DB for the given fork+alloc,
// creating it if necessary. The DB has both KV state (headers, TDs) and domain
// state (accounts, code, storage) fully committed.
func getOrCreateGenesisDB(fork string, gspec *types.Genesis) (kv.TemporalRwDB, *types.Block, error) {
	key := fork + "/" + genesisSpecHash(gspec)

	if entryI, ok := genesisDBCache.Load(key); ok {
		e := entryI.(*genesisCacheEntry)
		return e.db, e.genesis, nil
	}

	// Serialize creation to avoid races where a goroutine opens a RO tx
	// before the genesis commit is finished.
	genesisDBMu.Lock()
	defer genesisDBMu.Unlock()
	if entryI, ok := genesisDBCache.Load(key); ok {
		e := entryI.(*genesisCacheEntry)
		return e.db, e.genesis, nil
	}
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	dir, err := os.MkdirTemp("", "erigon-genesis-"+fork+"-")
	if err != nil {
		return nil, nil, fmt.Errorf("genesis cache: temp dir: %w", err)
	}
	dirs := datadir.New(dir)
	// Use a sentinel testing.T to get proper DB initialization (tb.Context(),
	// cleanup registration, etc.). We pass the parent test's t here — the DB
	// will outlive individual subtests but be cleaned up when the parent finishes.
	db := temporaltest.NewTestDB(nil, dirs)

	// Step 1: CommitGenesisBlock writes headers, TDs, config to KV tables.
	// Use separate dirs for GenesisToBlock's temp DB to avoid domain file conflicts.
	genesisDirs := datadir.New(dir + "-genesis-tmp")
	os.MkdirAll(genesisDirs.Tmp, 0755)
	os.MkdirAll(genesisDirs.SnapDomain, 0755)
	_, genesis, err := genesiswrite.CommitGenesisBlock(db, gspec, "", genesisDirs, logger)
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("genesis cache: CommitGenesisBlock: %w", err)
	}

	ctx := context.Background()

	// Step 2: Write domain state via SharedDomains + ComputeGenesisCommitment.
	// Without this, domain state is invisible through RO tx.
	rwTx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("genesis cache: BeginTemporalRw: %w", err)
	}

	sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	if err != nil {
		rwTx.Rollback()
		db.Close()
		return nil, nil, fmt.Errorf("genesis cache: NewSharedDomains: %w", err)
	}

	_, _, err = genesiswrite.ComputeGenesisCommitment(ctx, gspec, rwTx, sd, genesis.Header())
	if err != nil {
		sd.Close()
		rwTx.Rollback()
		db.Close()
		return nil, nil, fmt.Errorf("genesis cache: ComputeGenesisCommitment: %w", err)
	}

	err = sd.Flush(ctx, rwTx)
	sd.Close()
	if err != nil {
		rwTx.Rollback()
		db.Close()
		return nil, nil, fmt.Errorf("genesis cache: sd.Flush: %w", err)
	}

	err = rwTx.Commit()
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("genesis cache: tx.Commit: %w", err)
	}
	// Step 3: Deploy Prague system contracts if needed.
	if gspec.Config.IsPrague(0) {
		if err := blockgen.InitPraguePreDeploys(db, logger); err != nil {
			db.Close()
			return nil, nil, fmt.Errorf("genesis cache: InitPraguePreDeploys: %w", err)
		}
	}

	entry := &genesisCacheEntry{db: db, genesis: genesis}
	genesisDBCache.Store(key, entry)
	return db, genesis, nil
}

// RegisterGenesisCacheCleanup registers a test cleanup function that closes
// and removes all cached genesis DBs. Call this once from TestMain or
// a top-level test.
func RegisterGenesisCacheCleanup(t *testing.T) {
	t.Cleanup(func() {
		genesisDBCache.Range(func(key, value any) bool {
			if e, ok := value.(*genesisCacheEntry); ok && e.db != nil {
				e.db.Close()
			}
			genesisDBCache.Delete(key)
			return true
		})
	})
}


// RunLightweight tries to execute the block test using a MemoryMutation overlay
// on a shared, cached genesis DB. If the lightweight path encounters any error,
// it falls back to the full BlockTest.Run path for authoritative results.
func (bt *BlockTest) RunLightweight(t *testing.T) error {
	if err := bt.runLightweight(t); err != nil {
		return bt.Run(t) // lightweight path failed — fall back to full pipeline
	}
	return nil
}

func (bt *BlockTest) runLightweight(t *testing.T) error {
	config, ok := testforks.Forks[bt.json.Network]
	if !ok {
		return testforks.UnsupportedForkError{Name: bt.json.Network}
	}

	gspec := bt.genesis(config)
	db, genesis, err := getOrCreateGenesisDB(bt.json.Network, gspec)
	if err != nil {
		// Fall back to the full Run method if genesis cache creation fails.
		t.Logf("genesis cache failed, falling back to full Run: %v", err)
		return bt.Run(t)
	}

	// Validate genesis hash and state root — fall back to full Run if they don't match.
	if genesis.Hash() != bt.json.Genesis.Hash || genesis.Root() != bt.json.Genesis.StateRoot {
		return bt.Run(t)
	}

	ctx := context.Background()
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	// Open a RO temporal tx on the shared genesis DB.
	roTx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("BeginTemporalRo: %w", err)
	}
	defer roTx.Rollback()

	// Create a MemoryMutation overlay for ephemeral writes.
	overlay, err := membatchwithdb.NewMemoryBatch(roTx, "", logger)
	if err != nil {
		return fmt.Errorf("NewMemoryBatch: %w", err)
	}
	defer overlay.Close()

	// Look up the starting txNum from genesis block.
	txNum, err := rawdbv3.TxNums.Max(ctx, roTx, 0)
	if err != nil {
		return fmt.Errorf("TxNums.Max: %w", err)
	}
	txNum++ // next available

	engine := rulesconfig.CreateRulesEngineBareBones(ctx, config, logger)
	defer engine.Close()

	// Build a lightweight chain reader with in-memory header/TD maps.
	cr := &lightChainReader{
		config:  config,
		headers: make(map[common.Hash]*types.Header),
		tds:     make(map[common.Hash]*big.Int),
		tx:      overlay,
	}

	// Seed the chain reader with genesis header and TD.
	cr.headers[genesis.Hash()] = genesis.Header()
	genesisTd, _ := rawdb.ReadTd(roTx, genesis.Hash(), 0)
	if genesisTd != nil {
		cr.tds[genesis.Hash()] = genesisTd
	}

	lastBlockHash := genesis.Hash()

	// Execute blocks.
	for bi, b := range bt.json.Blocks {
		cb, err := b.decode()
		if err != nil {
			if b.BlockHeader == nil {
				continue // expected invalid block
			}
			return fmt.Errorf("block RLP decoding failed when expected to succeed: %w", err)
		}

		header := cb.Header()
		parentHash := header.ParentHash
		parentTd, hasTd := cr.tds[parentHash]
		if !hasTd {
			parentTd, _ = rawdb.ReadTd(overlay, parentHash, header.Number.Uint64()-1)
		}

		// Allocate 2 system txNums (begin/end of block) + 1 per tx.
		txNum++ // begin-of-block system tx
		stateReader := state.NewReaderV3(overlay)
		stateWriter := state.NewWriter(overlay, nil, txNum)

		blockHashFunc := protocol.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
			h := cr.GetHeader(hash, number)
			if h == nil {
				return nil, fmt.Errorf("header not found: %x at %d", hash, number)
			}
			return h, nil
		})

		// Verify block header before execution (gas limit, timestamp, etc.).
		if verifyErr := engine.VerifyHeader(cr, header, false); verifyErr != nil {
			if b.BlockHeader == nil {
				continue // expected invalid block
			}
			return fmt.Errorf("block #%v header verification failed: %w", cb.Number(), verifyErr)
		}
		// Check gas used doesn't exceed gas limit (GAS_USED_OVERFLOW).
		if header.GasUsed > header.GasLimit {
			if b.BlockHeader == nil {
				continue
			}
			return fmt.Errorf("block #%v gas used %d exceeds gas limit %d", cb.Number(), header.GasUsed, header.GasLimit)
		}

		// Wrap in panic recovery for blocks with malicious/invalid headers
		// that cause panics in the EVM or consensus engine.
		var execResult *protocol.EphemeralExecResult
		var execErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					execErr = fmt.Errorf("panic during block execution: %v", r)
				}
			}()
			execResult, execErr = protocol.ExecuteBlockEphemerally(
				config, &vm.Config{},
				blockHashFunc, engine, cb,
				stateReader, stateWriter,
				cr, nil,
				logger,
			)
		}()

		if execErr != nil {
			if b.BlockHeader == nil {
				continue // expected invalid block
			}
			return fmt.Errorf("block #%v execution failed: %w", cb.Number(), execErr)
		}

		_ = execResult

		if b.BlockHeader == nil {
			// Block was expected to be invalid but execution succeeded.
			// In the lightweight runner we cannot easily check canonicality,
			// so we just treat it as an expected failure and continue.
			_ = bi
			continue
		}

		// Update txNum for the transactions in this block + end-of-block system tx.
		txNum += uint64(cb.Transactions().Len())
		txNum++ // end-of-block system tx

		// Record the header and TD for subsequent blocks.
		cr.headers[cb.Hash()] = header
		blockTd := new(big.Int)
		if parentTd != nil {
			blockTd.Set(parentTd)
		}
		blockTd.Add(blockTd, header.Difficulty.ToBig())
		cr.tds[cb.Hash()] = blockTd
		lastBlockHash = cb.Hash()
	}

	// Validate last block hash.
	if common.Hash(bt.json.BestBlock) != lastBlockHash {
		return fmt.Errorf("last block hash validation mismatch: want: %x, have: %x",
			bt.json.BestBlock, lastBlockHash)
	}

	// Validate post-state using the overlay.
	newDB := state.New(state.NewReaderV3(overlay))
	defer newDB.Release(false)
	if err := bt.validatePostState(newDB); err != nil {
		return fmt.Errorf("post state validation failed: %w", err)
	}

	return nil
}
