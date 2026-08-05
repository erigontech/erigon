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

package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/seg"
)

// StateKeyWalker enumerates every unique key that had at least one write
// in a mode-C boundary regen's advertised window (fromTxN, lastTxN+1]. The
// callback returns false to stop iteration early. Implementations MUST
// deduplicate: a single yield call per unique key regardless of how many
// times that key was written in the window.
//
// Production wiring builds one from tx.Debug().HistoryKeyTxNumRange with
// adjacent-duplicate suppression (HistoryKeyTxNumRange emits per-write,
// asc-sorted by key-first-then-txNum, so consecutive duplicates come
// from the same key). Tests can wire it from a literal slice.
type StateKeyWalker func(yield func(key []byte) bool) error

// WriteStateBoundaryFileV4 emits a v4-named boundary .kv file for a state
// domain (accounts / storage / code / receipt) whose advertised range is
// [fromTxN, lastTxN+1) and whose content is state as-of lastTxN for every
// key touched in that window.
//
// Companion to WriteCommitmentBoundaryFileV4. Both restore the mode-C
// completeness invariant (advertised endTxN matches the content's true
// horizon). They differ in key-source: commitment uses the compute's
// captured branches (the trie fold visited every touched branch);
// state domains enumerate via the supplied walker (typically wired to
// tx.Debug().HistoryKeyTxNumRange with adjacent-duplicate suppression).
//
// The critical correctness property is "no dropped keys". The prior
// implementation iterated only the OLD boundary .kv file, which
// contained keys as-of some past step-end txN; any key first-written
// in (that past step-end, lastTxN] was missing from both the OLD file
// AND the v4 emit, and MDBX's writable-shadow value for it (post-wipe)
// was rejected by getLatestFromDb's rule (foundStep-end < files.EndTxN).
// GetLatest then fell to older files → not found → empty → forward-exec
// mis-priced the SSTORE → block invalidated. Walking history over the
// window catches every such key.
//
// Under --prune.mode=minimal the history for the walked window must be
// on-disk first; ensureHistoryForUnwindWalk arranges this via the
// preverified download path (see e7caf0ccf7).
//
// lookup is called once per unique key with ts=lastTxN. A (nil, false,
// nil) result is written as a value-domain tombstone (empty value) so
// older baseline files at earlier steps don't leak pre-tombstone values.
func WriteStateBoundaryFileV4(
	ctx context.Context,
	domain kv.Domain,
	keys StateKeyWalker,
	lookup AsOfLookup,
	lastTxN uint64,
	newKVPath string,
	tmpDir string,
	compression seg.FileCompression,
	logger log.Logger,
) error {
	if domain == kv.CommitmentDomain {
		return fmt.Errorf("WriteStateBoundaryFileV4: commitment domain must use WriteCommitmentBoundaryFileV4")
	}
	if keys == nil {
		return fmt.Errorf("WriteStateBoundaryFileV4: keys walker is required")
	}
	if lookup == nil {
		return fmt.Errorf("WriteStateBoundaryFileV4: lookup is required")
	}
	if newKVPath == "" {
		return fmt.Errorf("WriteStateBoundaryFileV4: newKVPath is required")
	}

	comp, err := seg.NewCompressor(ctx, "mode-C boundary-step regen", newKVPath, tmpDir, seg.DefaultCfg, log.LvlInfo, logger)
	if err != nil {
		return fmt.Errorf("create %s: %w", newKVPath, err)
	}
	defer comp.Close()
	writer := seg.NewWriter(comp, compression)

	var (
		kept    uint64
		emitErr error
	)
	walkErr := keys(func(key []byte) bool {
		v, found, lerr := lookup(domain, key, lastTxN)
		if lerr != nil {
			emitErr = fmt.Errorf("AsOfLookup(%s, key, %d): %w", domain, lastTxN, lerr)
			return false
		}
		if !found {
			v = nil // value-domain tombstone
		}
		if _, werr := writer.Write(key); werr != nil {
			emitErr = fmt.Errorf("write key: %w", werr)
			return false
		}
		if _, werr := writer.Write(v); werr != nil {
			emitErr = fmt.Errorf("write value: %w", werr)
			return false
		}
		kept++
		return true
	})
	if emitErr != nil {
		return emitErr
	}
	if walkErr != nil {
		return fmt.Errorf("walk history keys(%s): %w", domain, walkErr)
	}

	if err := comp.Compress(); err != nil {
		return fmt.Errorf("compress %s: %w", newKVPath, err)
	}

	if logger != nil {
		logger.Info("[storage] mode-C boundary-step regen v4",
			"domain", domain, "path", newKVPath, "lastTxN", lastTxN, "kept", kept)
	}
	return nil
}

// historyKeyWalker returns a StateKeyWalker backed by
// tx.Debug().HistoryKeyTxNumRange(domain, fromTxN, lastTxN+1). Same-key
// consecutive duplicates from the per-write stream are suppressed so the
// yield callback fires exactly once per unique key.
//
// The (fromTxN, lastTxN] window matches the WriteStateBoundaryFileV4
// file's advertised range [fromTxN, lastTxN+1). ensureHistoryForUnwindWalk
// is what guarantees the domain's .v file covers the window on disk.
func historyKeyWalker(tx kv.TemporalTx, domain kv.Domain, fromTxN, lastTxN uint64) StateKeyWalker {
	return func(yield func(key []byte) bool) error {
		it, err := tx.Debug().HistoryKeyTxNumRange(domain, int(fromTxN), int(lastTxN+1), order.Asc, -1)
		if err != nil {
			return fmt.Errorf("HistoryKeyTxNumRange(%s, [%d, %d)): %w", domain, fromTxN, lastTxN+1, err)
		}
		defer it.Close()
		var prevKey []byte
		for it.HasNext() {
			k, _, ierr := it.Next()
			if ierr != nil {
				return fmt.Errorf("HistoryKeyTxNumRange next(%s): %w", domain, ierr)
			}
			if prevKey != nil && bytes.Equal(k, prevKey) {
				continue
			}
			prevKey = append(prevKey[:0], k...)
			if !yield(k) {
				return nil
			}
		}
		return nil
	}
}
