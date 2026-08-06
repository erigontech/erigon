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
	"context"
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// WriteCommitmentBoundaryFileV4 emits a v4-named commitment .kv file
// whose content is the fresh branch set from a mode-C compute plus a
// KeyCommitmentState anchor blob — both authored at lastTxNum.
//
// The v4 file's advertised endTxN is lastTxN+1 (honest raw-txN naming
// per TxNumNamingPivot), so its content matches its horizon. This
// replaces the 2026-06-03 "regen in place at same step coordinates"
// hack that produced files whose filename advertised a step boundary
// but whose content was as-of-lastTxN — silently corrupting reads for
// state domains and surfacing as G7 for commitment (see memory/
// mode-c-subversion-and-v4-supersede-2026-08-03.md).
//
// Ordering: branches are hex-nibble-encoded (each byte in 0x00-0x0F);
// KeyCommitmentState is []byte("state") (starts 0x73). Every branch
// sorts before the anchor, so the emit is: drain branches in sorted
// order (etl.Collector.Load guarantee) then append (KeyCommitmentState,
// anchor).
//
// The caller (Provider.Unwind's regen phase) provides:
//   - branches: mode-C compute's captured branch set at lastTxNum.
//     Populated by ensureCommitmentAtBlockApply's drain-fork.
//   - anchor: encoded commitmentState (blockNum=toBlock, txNum=lastTxNum,
//     trieState) — same blob the writable-shadow apply writes at
//     KeyCommitmentState.
//   - newKVPath: destination for the .regen file. FinalizeUnwind
//     atomically renames <path>.regen → <path>.
//   - accessors: builds the .kvi (or .bt/.kvei under AGG_COMMITMENT_BT)
//     sidecar post-Compress. Required — see AccessorBuilder doc for the
//     invisibility failure mode this fixes.
func WriteCommitmentBoundaryFileV4(
	ctx context.Context,
	branches *etl.Collector,
	anchor []byte,
	newKVPath string,
	tmpDir string,
	compression seg.FileCompression,
	accessors AccessorBuilder,
	logger log.Logger,
) error {
	if branches == nil {
		return fmt.Errorf("WriteCommitmentBoundaryFileV4: branches collector is required")
	}
	if len(anchor) == 0 {
		return fmt.Errorf("WriteCommitmentBoundaryFileV4: anchor blob is required")
	}
	if newKVPath == "" {
		return fmt.Errorf("WriteCommitmentBoundaryFileV4: newKVPath is required")
	}
	if accessors == nil {
		return fmt.Errorf("WriteCommitmentBoundaryFileV4: accessors builder is required (v4 .kv without accessors is invisible to state reads)")
	}

	comp, err := seg.NewCompressor(ctx, "mode-C commitment v4 emit", newKVPath, tmpDir, seg.DefaultCfg, log.LvlInfo, logger)
	if err != nil {
		return fmt.Errorf("create %s: %w", newKVPath, err)
	}
	defer comp.Close()
	writer := seg.NewWriter(comp, compression)

	var branchCount uint64
	loadErr := branches.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		if _, err := writer.Write(k); err != nil {
			return fmt.Errorf("write branch key: %w", err)
		}
		if _, err := writer.Write(v); err != nil {
			return fmt.Errorf("write branch value: %w", err)
		}
		branchCount++
		return nil
	}, etl.TransformArgs{})
	if loadErr != nil {
		return fmt.Errorf("drain branches into v4 file: %w", loadErr)
	}

	if _, err := writer.Write(commitmentdb.KeyCommitmentState); err != nil {
		return fmt.Errorf("write KeyCommitmentState key: %w", err)
	}
	if _, err := writer.Write(anchor); err != nil {
		return fmt.Errorf("write KeyCommitmentState anchor: %w", err)
	}

	if err := comp.Compress(); err != nil {
		return fmt.Errorf("compress %s: %w", newKVPath, err)
	}

	finalKVPath := strings.TrimSuffix(newKVPath, ".regen")
	if err := accessors.BuildKVAccessors(ctx, kv.CommitmentDomain, newKVPath, finalKVPath); err != nil {
		return fmt.Errorf("build v4 commitment accessors for %s (final=%s): %w", newKVPath, finalKVPath, err)
	}

	if logger != nil {
		logger.Info("[storage] mode-C commitment v4 emit",
			"path", newKVPath, "branches", branchCount)
	}
	return nil
}
