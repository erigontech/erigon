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
	"strings"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/seg"
)

// v4EmitTrace toggles per-key trace logging for the v4-emit walker
// and downstream lookups. Off by default (production overhead is
// nil). Enable via ERIGON_V4EMIT_TRACE=true on a soak binary to
// capture the yielded key set + resolved values for the failing
// unwind profile — used to diagnose the state-v4 non-determinism
// class (mode-c-v4-emit-nondeterministic-2026-08-06).
var v4EmitTrace = dbg.EnvBool("ERIGON_V4EMIT_TRACE", false)

// AccessorBuilder builds the .bt / .kvei / .kvi sidecars that a domain
// .kv needs to be visible to state reads. Every mode-C v4 emit MUST
// supply one — without accessors the emitted .kv is on disk but
// excluded from every DomainRoTx visible set (checkForVisibility in
// db/state/dirty_files.go rejects items whose bindex or existence
// filter is nil), and forward-exec reads bypass v4 → falls through to
// older files → returns pre-window state → mis-priced SSTOREs by tens
// to hundreds of kilo-gas → block invalidated.
//
// dataPath is where the .kv was actually written (typically a .regen
// suffix during Provider.Unwind); finalPath is the eventual name the
// accessor filenames get derived from (so accessors land at the paired
// name FinalizeUnwind will rename the .kv to). The two are the same
// when the writer targets the final path directly.
type AccessorBuilder interface {
	BuildKVAccessors(ctx context.Context, domain kv.Domain, dataPath, finalPath string) error
}

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
//
// accessors builds the .bt/.kvei/.kvi sidecars post-Compress. Required —
// without them the emitted .kv is invisible to state reads (see the
// AccessorBuilder doc for the failure mode this fixes).
func WriteStateBoundaryFileV4(
	ctx context.Context,
	domain kv.Domain,
	keys StateKeyWalker,
	lookup AsOfLookup,
	lastTxN uint64,
	newKVPath string,
	tmpDir string,
	compression seg.FileCompression,
	accessors AccessorBuilder,
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
	if accessors == nil {
		return fmt.Errorf("WriteStateBoundaryFileV4: accessors builder is required (v4 .kv without accessors is invisible to state reads)")
	}

	comp, err := seg.NewCompressor(ctx, "mode-C boundary-step regen", newKVPath, tmpDir, seg.DefaultCfg, log.LvlInfo, logger)
	if err != nil {
		return fmt.Errorf("create %s: %w", newKVPath, err)
	}
	defer comp.Close()
	writer := seg.NewWriter(comp, compression)

	var (
		kept       uint64
		emitErr    error
		trace      = v4EmitTrace
		tombstones uint64
	)
	walkErr := keys(func(key []byte) bool {
		v, found, lerr := lookup(domain, key, lastTxN)
		if lerr != nil {
			emitErr = fmt.Errorf("AsOfLookup(%s, key, %d): %w", domain, lastTxN, lerr)
			return false
		}
		if !found {
			v = nil // value-domain tombstone
			tombstones++
		}
		if trace {
			log.Warn("[v4emit-trace] emit.entry",
				"domain", domain,
				"key", fmt.Sprintf("%x", key),
				"vlen", len(v),
				"found", found,
				"lastTxN", lastTxN)
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
	if trace {
		log.Warn("[v4emit-trace] emit.done",
			"domain", domain,
			"kept", kept, "tombstones", tombstones,
			"path", newKVPath)
	}
	if emitErr != nil {
		return emitErr
	}
	if walkErr != nil {
		return fmt.Errorf("walk history keys(%s): %w", domain, walkErr)
	}

	if err := comp.Compress(); err != nil {
		return fmt.Errorf("compress %s: %w", newKVPath, err)
	}
	// Compress renames its temp file to newKVPath and releases the fd
	// internally; the accessor builder can seg.NewDecompressor(newKVPath)
	// directly. The final-naming trim lets callers pass a .regen path
	// while accessors land at the eventual final .bt/.kvei/.kvi names.
	finalKVPath := strings.TrimSuffix(newKVPath, ".regen")
	if err := accessors.BuildKVAccessors(ctx, domain, newKVPath, finalKVPath); err != nil {
		return fmt.Errorf("build v4 accessors for %s (final=%s): %w", newKVPath, finalKVPath, err)
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
		trace := v4EmitTrace
		var (
			prevKey      []byte
			raw, yielded uint64
			firstTxN     uint64 = ^uint64(0)
			lastSeenTxN  uint64
		)
		for it.HasNext() {
			k, txN, ierr := it.Next()
			if ierr != nil {
				return fmt.Errorf("HistoryKeyTxNumRange next(%s): %w", domain, ierr)
			}
			raw++
			if txN < firstTxN {
				firstTxN = txN
			}
			if txN > lastSeenTxN {
				lastSeenTxN = txN
			}
			if prevKey != nil && bytes.Equal(k, prevKey) {
				if trace {
					log.Warn("[v4emit-trace] walker.dup", "domain", domain, "key", fmt.Sprintf("%x", k), "txN", txN)
				}
				continue
			}
			prevKey = append(prevKey[:0], k...)
			yielded++
			if trace {
				log.Warn("[v4emit-trace] walker.yield", "domain", domain, "key", fmt.Sprintf("%x", k), "txN", txN)
			}
			if !yield(k) {
				return nil
			}
		}
		if trace {
			log.Warn("[v4emit-trace] walker.done",
				"domain", domain,
				"range", fmt.Sprintf("(%d,%d]", fromTxN, lastTxN),
				"raw", raw, "yielded", yielded,
				"firstTxN", firstTxN, "lastSeenTxN", lastSeenTxN)
		}
		return nil
	}
}
