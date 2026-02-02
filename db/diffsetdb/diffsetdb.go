// Copyright 2024 The Erigon Authors
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

package diffsetdb

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

const diffsetExt = ".diffset"

// Pre-computed hex lookup table for zero-allocation hex encoding
var hexTable = [16]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}

var diffsetDBByDir sync.Map

// DiffsetDatabase stores per-block diffsets as files in the datadir.
type DiffsetDatabase struct {
	dir string     // absolute path to datadir diffsets folder
	mu  sync.Mutex // guards file reads/writes

	// Auxiliary buffers for zero-allocation serialization
	serializeBuf []byte // reusable buffer for serialization
	pathBuf      []byte // reusable buffer for file path construction
	filenameBuf  []byte // reusable buffer for filename construction
}

func Open(dirs datadir.Dirs) *DiffsetDatabase {
	if dirs.DataDir == "" {
		return &DiffsetDatabase{}
	}
	key := filepath.Clean(dirs.DataDir)
	if cached, ok := diffsetDBByDir.Load(key); ok {
		return cached.(*DiffsetDatabase)
	}
	db := &DiffsetDatabase{
		dir:          dirs.Diffsets,
		serializeBuf: make([]byte, 0, 1024*1024), // 1MB initial capacity
		pathBuf:      make([]byte, 0, 256),
		filenameBuf:  make([]byte, 0, 128),
	}
	actual, _ := diffsetDBByDir.LoadOrStore(key, db)
	return actual.(*DiffsetDatabase)
}

// Dir returns the diffset directory path.
func (db *DiffsetDatabase) Dir() string {
	return db.dir
}

// WriteDiffSet serializes and writes a block diffset to its per-block file.
func (db *DiffsetDatabase) WriteDiffSet(blockNumber uint64, blockHash common.Hash, diffSet *changeset.StateChangeSet) error {
	if diffSet == nil {
		return errors.New("diffset is nil")
	}
	if err := db.ensureDir(); err != nil {
		return err
	}

	go func() {
		db.mu.Lock()
		defer db.mu.Unlock()

		if dbg.TraceUnwinds {
			var diffStats strings.Builder
			first := true
			for d, diff := range &diffSet.Diffs {
				if first {
					diffStats.WriteString(" ")
					first = false
				} else {
					diffStats.WriteString(", ")
				}
				diffStats.WriteString(fmt.Sprintf("%s: %d", kv.Domain(d), diff.Len()))
			}
			fmt.Printf("diffset (Block:%d) %x:%s %s\n", blockNumber, blockHash, diffStats.String(), dbg.Stack())
		}

		// Serialize the diffset
		db.serializeBuf = diffSet.SerializeTo(db.serializeBuf[:0], blockNumber)

		// Build path using auxiliary buffers
		path := db.diffsetPathZeroAlloc(blockNumber, blockHash)

		if err := dir.WriteFileWithFsync(path, db.serializeBuf, 0o644); err != nil {
			log.Error("DiffsetDatabase.WriteDiffSet: write error", "blockNumber", blockNumber, "blockHash", blockHash, "err", err)
		}
	}()
	return nil
}

// ReadDiffSet loads a block diffset from its per-block file.
func (db *DiffsetDatabase) ReadDiffSet(blockNumber uint64, blockHash common.Hash) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
	if db.dir == "" {
		return [kv.DomainLen][]kv.DomainEntryDiff{}, false, errors.New("diffset database requires datadir")
	}
	path := db.diffsetPath(blockNumber, blockHash)
	db.mu.Lock()
	data, err := os.ReadFile(path)
	db.mu.Unlock()
	if err != nil {
		if os.IsNotExist(err) {
			return [kv.DomainLen][]kv.DomainEntryDiff{}, false, nil
		}
		return [kv.DomainLen][]kv.DomainEntryDiff{}, false, err
	}
	if len(data) == 0 {
		return [kv.DomainLen][]kv.DomainEntryDiff{}, false, nil
	}
	return changeset.DeserializeStateChangeSet(data), true, nil
}

// ReadLowestUnwindableBlock scans diffset files and returns the smallest block number found.
func (db *DiffsetDatabase) ReadLowestUnwindableBlock() (uint64, error) {
	if db.dir == "" {
		return math.MaxUint64, errors.New("diffset database requires datadir")
	}
	db.mu.Lock()
	entries, err := os.ReadDir(db.dir)
	db.mu.Unlock()
	if err != nil {
		if os.IsNotExist(err) {
			return math.MaxUint64, nil
		}
		return 0, err
	}
	minBlock := uint64(math.MaxUint64)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		blockNum, _, ok := parseFileName(entry.Name())
		if !ok {
			continue
		}
		if blockNum < minBlock {
			minBlock = blockNum
		}
	}
	return minBlock, nil
}

// PruneBefore removes diffset files with block numbers lower than cutoff.
func (db *DiffsetDatabase) PruneBefore(ctx context.Context, cutoff uint64, limit int, timeout time.Duration) (int, error) {
	if db.dir == "" {
		return 0, errors.New("diffset database requires datadir")
	}
	start := time.Now()
	removed := 0
	deadline := time.Time{}
	if timeout > 0 {
		deadline = start.Add(timeout)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entries, err := os.ReadDir(db.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	for _, entry := range entries {
		if limit >= 0 && removed >= limit {
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return removed, ctx.Err()
			default:
			}
		}
		if entry.IsDir() {
			continue
		}
		blockNum, _, ok := parseFileName(entry.Name())
		if !ok || blockNum >= cutoff {
			continue
		}
		if err := dir.RemoveFile(filepath.Join(db.dir, entry.Name())); err != nil && !os.IsNotExist(err) {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// Clear deletes the entire diffset directory.
func (db *DiffsetDatabase) Clear() error {
	if db.dir == "" {
		return errors.New("diffset database requires datadir")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return dir.RemoveAll(db.dir)
}

// ensureDir creates the diffset directory if it does not exist.
func (db *DiffsetDatabase) ensureDir() error {
	if db.dir == "" {
		return errors.New("diffset database requires datadir")
	}
	return os.MkdirAll(db.dir, dir.DirPerm)
}

// diffsetPath builds the path for a given block's diffset file.
func (db *DiffsetDatabase) diffsetPath(blockNumber uint64, blockHash common.Hash) string {
	return filepath.Join(db.dir, diffsetFileName(blockNumber, blockHash))
}

// diffsetPathZeroAlloc builds the path using pre-allocated buffers (zero-alloc).
// Must be called under lock as it uses shared buffers.
func (db *DiffsetDatabase) diffsetPathZeroAlloc(blockNumber uint64, blockHash common.Hash) string {
	// Build filename into filenameBuf
	db.filenameBuf = db.filenameBuf[:0]
	db.filenameBuf = strconv.AppendUint(db.filenameBuf, blockNumber, 10)
	db.filenameBuf = append(db.filenameBuf, '+')
	// Hex encode the hash directly into buffer
	for _, b := range blockHash {
		db.filenameBuf = append(db.filenameBuf, hexTable[b>>4], hexTable[b&0x0f])
	}
	db.filenameBuf = append(db.filenameBuf, diffsetExt...)

	// Build full path
	db.pathBuf = db.pathBuf[:0]
	db.pathBuf = append(db.pathBuf, db.dir...)
	db.pathBuf = append(db.pathBuf, filepath.Separator)
	db.pathBuf = append(db.pathBuf, db.filenameBuf...)

	return string(db.pathBuf)
}

// diffsetFileName formats the on-disk filename for a block diffset.
func diffsetFileName(blockNumber uint64, blockHash common.Hash) string {
	return fmt.Sprintf("%d+%x%s", blockNumber, blockHash[:], diffsetExt)
}

// parseFileName extracts block number and hash from a diffset filename.
func parseFileName(name string) (uint64, common.Hash, bool) {
	if !strings.HasSuffix(name, diffsetExt) {
		return 0, common.Hash{}, false
	}
	trimmed := strings.TrimSuffix(name, diffsetExt)
	parts := strings.SplitN(trimmed, "+", 2)
	if len(parts) != 2 {
		return 0, common.Hash{}, false
	}
	blockNum, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, common.Hash{}, false
	}
	decoded, err := hex.DecodeString(parts[1])
	if err != nil || len(decoded) != length.Hash {
		return 0, common.Hash{}, false
	}
	var hash common.Hash
	copy(hash[:], decoded)
	return blockNum, hash, true
}
