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
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// AsOfLookup resolves the value of (domain, key) valid at lastTxNum.
// Returns (val=nil, found=false) for keys that didn't exist at
// lastTxNum (created strictly after). Returns (val, true) for keys
// whose value at lastTxNum can be reconstructed from history +
// current state.
//
// Phase 3 wires this to kv.TemporalTx.HistorySeek (or GetAsOf — TBD
// based on the precise "AT txNum" semantic the boundary-step regen
// needs; Phase 4 will pin the choice via end-to-end verify). For
// unit tests, callers supply a synthetic lookup.
type AsOfLookup func(domain kv.Domain, key []byte, lastTxNum uint64) (val []byte, found bool, err error)

// RegenerateBoundaryStepFile rewrites the given boundary-step state
// snapshot file so every (k, v) pair reflects the value of k valid at
// lastTxNum, replacing post-anchor stale entries with their as-of-
// lastTxNum value via the supplied AsOfLookup. Keys that didn't exist
// at lastTxNum are dropped. For the commitment domain, the
// KeyCommitmentState entry is replaced with the supplied
// commitmentAnchor blob unconditionally — that's the mode-B
// post-state anchor encoding (blockNum=toBlock, txNum=lastTxNum,
// trieState).
//
// The new file is written to <originalPath>.regen — NOT to the
// original path. Phase 3's FinalizeUnwind handles the atomic rename
// (and AbortUnwind handles the .regen cleanup on rollback) so a
// mid-call crash never leaves the original file in a partially-
// written state.
//
// Caller is responsible for:
//   - supplying compression matching the domain's DomainCfg.Compression
//     (so the new file's wire format matches what readers expect),
//   - rebuilding accessor files (.kvi / .bt / .kvei) against the
//     regenerated .kv after a successful return (NOT done here so
//     callers can batch / parallelise the accessor builds),
//   - staging the new + old file paths for FinalizeUnwind /
//     AbortUnwind.
//
// commitmentAnchor MUST be non-nil when domain == kv.CommitmentDomain
// (the whole point of the regen is to plant a fresh anchor) and MUST
// be nil otherwise (defensive — callers shouldn't be plumbing
// commitment-specific blobs for non-commitment domains).
func RegenerateBoundaryStepFile(
	ctx context.Context,
	domain kv.Domain,
	oldKVPath string,
	lookup AsOfLookup,
	lastTxNum uint64,
	compression seg.FileCompression,
	commitmentAnchor []byte,
	tmpDir string,
	logger log.Logger,
) (newPath string, err error) {
	if domain == kv.CommitmentDomain && commitmentAnchor == nil {
		return "", fmt.Errorf("RegenerateBoundaryStepFile(commitment): commitmentAnchor required")
	}
	if domain != kv.CommitmentDomain && commitmentAnchor != nil {
		return "", fmt.Errorf("RegenerateBoundaryStepFile(%s): commitmentAnchor must be nil for non-commitment domains", domain)
	}
	if lookup == nil {
		return "", fmt.Errorf("RegenerateBoundaryStepFile: lookup is required")
	}

	oldDecomp, err := seg.NewDecompressor(oldKVPath)
	if err != nil {
		return "", fmt.Errorf("open old %s: %w", oldKVPath, err)
	}
	defer oldDecomp.Close()

	newPath = oldKVPath + ".regen"
	compressCfg := seg.DefaultCfg
	comp, err := seg.NewCompressor(ctx, "mode-B boundary-step regen", newPath, tmpDir, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return "", fmt.Errorf("create new %s: %w", newPath, err)
	}
	defer comp.Close()

	reader := seg.NewReader(oldDecomp.MakeGetter(), compression)
	writer := seg.NewWriter(comp, compression)

	var (
		keyBuf, valBuf []byte
		kept, dropped  uint64
		anchorPlanted  bool
	)
	for reader.HasNext() {
		keyBuf, _ = reader.Next(keyBuf[:0])
		if !reader.HasNext() {
			return "", fmt.Errorf("malformed %s: trailing key with no value", oldKVPath)
		}
		valBuf, _ = reader.Next(valBuf[:0])

		// Commitment-domain special case: KeyCommitmentState is the
		// anchor record. Always plant the supplied anchor blob —
		// regardless of what the old file held — because mode-B's
		// whole point is to make this record reflect the unwind
		// target.
		if domain == kv.CommitmentDomain && bytes.Equal(keyBuf, commitmentdb.KeyCommitmentState) {
			if _, err := writer.Write(keyBuf); err != nil {
				return "", fmt.Errorf("write KeyCommitmentState key: %w", err)
			}
			if _, err := writer.Write(commitmentAnchor); err != nil {
				return "", fmt.Errorf("write KeyCommitmentState anchor: %w", err)
			}
			anchorPlanted = true
			kept++
			continue
		}

		// Resolve the value as-of lastTxNum. If absent, the key
		// didn't exist at the anchor point — drop from output.
		newVal, found, err := lookup(domain, keyBuf, lastTxNum)
		if err != nil {
			return "", fmt.Errorf("AsOfLookup(%s, key, %d): %w", domain, lastTxNum, err)
		}
		if !found {
			dropped++
			continue
		}

		if _, err := writer.Write(keyBuf); err != nil {
			return "", fmt.Errorf("write key: %w", err)
		}
		if _, err := writer.Write(newVal); err != nil {
			return "", fmt.Errorf("write value: %w", err)
		}
		kept++
	}

	// Commitment files MUST end up with a KeyCommitmentState record.
	// If the old file didn't have one (shouldn't happen in practice
	// but is defensible to detect) we'd silently emit a file the
	// chain can't seek a commitment from. Make that loud.
	if domain == kv.CommitmentDomain && !anchorPlanted {
		return "", fmt.Errorf("RegenerateBoundaryStepFile(commitment): old file %s had no KeyCommitmentState entry; regen would produce a commitment file with no anchor", oldKVPath)
	}

	if err := comp.Compress(); err != nil {
		return "", fmt.Errorf("compress %s: %w", newPath, err)
	}

	if logger != nil {
		logger.Info("[storage] mode-B boundary-step regen",
			"domain", domain, "old", oldKVPath, "new", newPath,
			"kept", kept, "dropped", dropped, "anchor_planted", anchorPlanted)
	}

	return newPath, nil
}
