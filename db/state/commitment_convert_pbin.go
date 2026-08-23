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

package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// A pre-version branch record opens with the high byte of its touchMap, always
// zero; a current one opens with a cell-fields byte, which always carries a kind
// bit. One byte separates the two formats without decoding either.
func pbinRecordIsLegacy(value []byte) bool { return len(value) > 0 && value[0] == 0 }

// convertPBinFile rewrites one commitment .kv from the pre-version record format
// into dstDir, accessors included. Keys are untouched — only values change.
func convertPBinFile(
	ctx context.Context,
	at *AggregatorRoTx,
	file VisibleFile,
	dstDir string,
	fileIdx, fileTotal int,
	grandTotalKeys, processedKeys uint64,
	logger log.Logger,
) (sizeDelta int64, deltaPct float32, ki uint64, err error) {
	vf, ok := file.(visibleFile)
	if !ok {
		return 0, 0, 0, fmt.Errorf("convertPBinFile %q: VisibleFile is not state.visibleFile (got %T)", file.Fullpath(), file)
	}
	src := vf.src
	if src == nil || src.decompressor == nil {
		return 0, 0, 0, fmt.Errorf("convertPBinFile %q: source has no decompressor", file.Fullpath())
	}

	commitmentRo := at.d[kv.CommitmentDomain]
	stepSize := at.StepSize()
	stepFrom, stepTo := kv.Step(file.StartRootNum()/stepSize), kv.Step(file.EndRootNum()/stepSize)

	srcCompression := commitmentRo.d.Compression
	if src.StepCount(stepSize) < DomainMinStepsToCompress {
		srcCompression = seg.CompressNone
	}
	reader := seg.NewReader(src.decompressor.MakeGetter(), srcCompression)
	reader.Reset(0)

	batch := &TemporalMemBatch{}
	batch.domainWriters[kv.CommitmentDomain] = commitmentRo.NewWriter()
	wal := batch.domainWriters[kv.CommitmentDomain]
	defer wal.Close()

	conv := commitment.NewPBinRecordConverter()
	baseName := filepath.Base(file.Fullpath())
	fileStart := time.Now()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	var k, v []byte
	var sawLegacy bool
	for reader.HasNext() {
		k, _ = reader.Next(k[:0])
		if !reader.HasNext() {
			return 0, 0, ki, fmt.Errorf("convertPBinFile %q: truncated at ki=%d (value missing)", file.Fullpath(), ki)
		}
		v, _ = reader.Next(v[:0])
		ki++

		var outVal []byte
		switch {
		case bytes.Equal(k, commitmentdb.KeyCommitmentState):
			if commitment.ValidatePBinStateFormat(v) == nil {
				outVal = append([]byte(nil), v...) // already current
				break
			}
			if outVal, err = conv.ConvertState(v); err != nil {
				return 0, 0, ki, fmt.Errorf("convertPBinFile %q: state record: %w", file.Fullpath(), err)
			}
			sawLegacy = true
		case pbinRecordIsLegacy(v):
			sawLegacy = true
			if outVal, err = conv.ConvertBranch(k, v); err != nil {
				return 0, 0, ki, fmt.Errorf("convertPBinFile %q: record at ki=%d key=%x: %w", file.Fullpath(), ki, k, err)
			}
		default:
			outVal = append([]byte(nil), v...)
		}

		if perr := wal.PutWithPrev(append([]byte(nil), k...), outVal, file.EndRootNum(), nil); perr != nil {
			return 0, 0, ki, fmt.Errorf("convertPBinFile %q: wal put at ki=%d: %w", file.Fullpath(), ki, perr)
		}

		select {
		case <-ctx.Done():
			return 0, 0, ki, ctx.Err()
		case <-logEvery.C:
			logger.Info(fmt.Sprintf("[pbin_convert] phase 1 file=%s %s key/s at %s/%s %s",
				baseName, formatRate(ki, time.Since(fileStart)),
				common.PrettyCounter(processedKeys+ki), common.PrettyCounter(grandTotalKeys),
				buildPhase1Prefix(fileIdx, fileTotal, processedKeys+ki, grandTotalKeys)))
		default:
		}
	}

	if !sawLegacy {
		return 0, 0, ki, errSkip
	}
	if err = commitmentRo.d.dumpStepRangeToPath(ctx, stepFrom, stepTo, batch, nil, dstDir, false); err != nil {
		return 0, 0, ki, fmt.Errorf("convertPBinFile %q: dumpStepRangeToPath: %w", file.Fullpath(), err)
	}
	newPath := commitmentRo.d.kvNewFilePathIn(dstDir, stepFrom, stepTo)
	if sizeDelta, deltaPct, err = commitmentFileSizeDelta(file.Fullpath(), newPath); err != nil {
		return 0, 0, ki, fmt.Errorf("convertPBinFile %q: size delta: %w", file.Fullpath(), err)
	}
	return sizeDelta, deltaPct, ki, nil
}

// ConvertPBinRecordFiles rewrites every pre-version pbin commitment file in the
// datadir to the current record format, in place: converted shards are built in
// snapshots/rebuild/domain/, the originals move to snapshots/backup/domains/,
// and the new files are promoted. A file already in the current format is left
// alone.
func ConvertPBinRecordFiles(ctx context.Context, at *AggregatorRoTx, logger log.Logger) error {
	allFiles := at.Files(kv.CommitmentDomain)
	files := make(VisibleFiles, 0, len(allFiles))
	for _, f := range allFiles {
		if strings.HasSuffix(f.Fullpath(), ".kv") {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		logger.Info("[pbin_convert] no commitment files to convert")
		return nil
	}

	dirs := at.Dirs()
	rebuildDir := filepath.Join(dirs.Snap, "rebuild", "domain")
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	if err := preflightBackupDir(backupDir); err != nil {
		return err
	}
	if err := os.MkdirAll(rebuildDir, 0o755); err != nil {
		return fmt.Errorf("[pbin_convert] mkdir rebuild dir %s: %w", rebuildDir, err)
	}

	var grandTotalKeys uint64
	for _, f := range files {
		grandTotalKeys += at.KeyCountInFiles(kv.CommitmentDomain, f.StartRootNum(), f.EndRootNum())
	}

	phaseStart := time.Now()
	var processedFiles, skippedFiles int
	var totalSizeDelta int64
	var processedKeys uint64
	for i, f := range files {
		delta, pct, ki, err := convertPBinFile(ctx, at, f, rebuildDir, i, len(files), grandTotalKeys, processedKeys, logger)
		processedKeys += ki
		if err != nil {
			if errors.Is(err, errSkip) {
				skippedFiles++
				logger.Info("[pbin_convert] already current", "file", filepath.Base(f.Fullpath()))
				continue
			}
			return err
		}
		processedFiles++
		totalSizeDelta += delta
		logger.Info("[pbin_convert] converted", "file", filepath.Base(f.Fullpath()),
			"keys", common.PrettyCounter(ki), "sizeDelta", signedByteSizeHR(delta),
			"pct", fmt.Sprintf("%.2f%%", pct))
	}
	logger.Info(fmt.Sprintf("[pbin_convert] phase 1 complete: converted %d, skipped %d, keys=%s in %s, sizeDelta=%s",
		processedFiles, skippedFiles, common.PrettyCounter(processedKeys),
		time.Since(phaseStart).Round(time.Second), signedByteSizeHR(totalSizeDelta)))

	if processedFiles == 0 {
		if rmErr := dir.RemoveAll(rebuildDir); rmErr != nil {
			logger.Warn("[pbin_convert] failed to remove empty rebuild dir", "path", rebuildDir, "err", rmErr)
		}
		cleanupParentIfEmpty(filepath.Dir(rebuildDir), logger)
		logger.Info("[pbin_convert] every file was already in the current format")
		return nil
	}

	convertedFiles, err := convertPhase2(at, files, rebuildDir)
	if err != nil {
		return err
	}
	if len(convertedFiles) != processedFiles {
		return fmt.Errorf("[pbin_convert] phase 2 mismatch: converted %d, found %d in rebuild dir",
			processedFiles, len(convertedFiles))
	}

	// Windows cannot rename a mmapped file, so the aggregator's handles on the
	// originals go before phase 3 moves them. That invalidates at until the
	// reload below republishes; only cached scalars are safe until then.
	stepSize := at.StepSize()
	at.a.closeDirtyFilesNoReopen()

	movedToBackup, err := convertPhase3(dirs.SnapDomain, backupDir, convertedFiles, stepSize)
	if err != nil {
		return err
	}
	promoted, err := convertPhase4(rebuildDir, dirs.SnapDomain)
	if err != nil {
		return err
	}
	if rmErr := dir.RemoveAll(rebuildDir); rmErr != nil {
		logger.Warn("[pbin_convert] failed to remove empty rebuild dir", "path", rebuildDir, "err", rmErr)
	}
	cleanupParentIfEmpty(filepath.Dir(rebuildDir), logger)
	if reloadErr := at.a.ReloadFiles(); reloadErr != nil {
		return fmt.Errorf("[pbin_convert] ReloadFiles: %w", reloadErr)
	}
	logger.Info(fmt.Sprintf(
		"[pbin_convert] DONE. converted %d files, %d backed up, %d promoted. Originals preserved at:\n    %s\nTo restore originals: integration commitment convert --restore",
		processedFiles, movedToBackup, promoted, backupDir))
	return nil
}
