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
	"os"
	"path/filepath"
	"sort"

	dirutil "github.com/erigontech/erigon/common/dir"
	"sync"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// genesisCacheEntry holds a cached genesis DB and its genesis block.
type genesisCacheEntry struct {
	db      kv.TemporalRwDB
	genesis *types.Block
	dirs    []string // temp directories to remove on cleanup
}

// genesisDBCache caches one MDBX database per unique (fork, genesisSpecHash) pair.
// The DB contains genesis KV state (headers, TDs, config) plus domain state
// (account balances, code, storage) written via SharedDomains.
// The cache is bounded to maxGenesisCacheSize entries to avoid exhausting disk.
const maxGenesisCacheSize = 128

var (
	genesisDBCache    sync.Map   // map[string]*genesisCacheEntry
	genesisDBMu       sync.Mutex // serializes genesis DB creation
	genesisCacheCount int        // approximate count, protected by genesisDBMu
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
	// Bound the cache to avoid exhausting disk on CI ramdisks.
	if genesisCacheCount >= maxGenesisCacheSize {
		return nil, nil, fmt.Errorf("genesis cache full (%d entries)", genesisCacheCount)
	}

	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	// Use a deterministic path so the same genesis DB can be found if the
	// process creates it in a previous test package run.  This also keeps
	// the number of temp dirs bounded by the number of unique genesis specs.
	dir := filepath.Join(os.TempDir(), "erigon-genesis-"+key)
	if err := os.MkdirAll(dir, 0755); err != nil {
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
	_ = os.MkdirAll(genesisDirs.Tmp, 0755)
	_ = os.MkdirAll(genesisDirs.SnapDomain, 0755)
	var genesis *types.Block
	var err error
	err = func() (gerr error) {
		defer func() {
			if r := recover(); r != nil {
				gerr = fmt.Errorf("panic: %v", r)
			}
		}()
		_, genesis, gerr = genesiswrite.CommitGenesisBlock(db, gspec, "", genesisDirs, logger)
		return
	}()
	// Clean up GenesisToBlock's temp DB immediately — it used a 2TB MDBX map
	// that exhausts CI ramdisks if left around.
	dirutil.RemoveAll(genesisDirs.DataDir)
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
	defer rwTx.Rollback() //nolint:gocritic

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

	entry := &genesisCacheEntry{db: db, genesis: genesis, dirs: []string{dir}}
	genesisDBCache.Store(key, entry)
	genesisCacheCount++
	return db, genesis, nil
}

var cleanupOnce sync.Once

// registerGenesisCacheCleanup ensures cleanup is registered exactly once.
func registerGenesisCacheCleanup(t *testing.T) {
	cleanupOnce.Do(func() {
		t.Cleanup(func() {
			genesisDBCache.Range(func(key, value any) bool {
				if e, ok := value.(*genesisCacheEntry); ok {
					if e.db != nil {
						e.db.Close()
					}
					for _, d := range e.dirs {
						dirutil.RemoveAll(d)
					}
				}
				genesisDBCache.Delete(key)
				return true
			})
		})
	})
}
