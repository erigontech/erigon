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
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
)

// TruncateStraddlerHistoryFile reads (oldEFPath, oldVPath) and writes filtered
// copies to (newEFPath, newVPath) that keep only history entries with
// txN <= targetTxN. Preserves the source file format (multiencseq .ef;
// paged .v) and page size (auto-detected from the source .v decompressor).
//
// baseTxN is the base txNum encoded in the source .ef sequences (the source
// file's startTxNum). For the mode-C v4 pairing the destination shares this
// base — the v4 range (baselineTxN, targetTxN] starts where the source step
// starts, and only trims the tail.
//
// Emits valid empty files when no source entry survives the filter. The
// output .ef preserves ascending-key order; keys whose sequences become empty
// under the filter are dropped entirely.
func TruncateStraddlerHistoryFile(
	ctx context.Context,
	oldEFPath, oldVPath string,
	newEFPath, newVPath string,
	baseTxN, targetTxN uint64,
	efCompression, vCompression seg.FileCompression,
	tmpDir string,
	logger log.Logger,
) (err error) {
	// Assert baseTxN agrees with the destination file names for v4
	// outputs — the reader parses fromTxN back from the file name and
	// uses it as seq.Reset base. A mismatch here silently produces
	// out-of-bounds Seek returns downstream (cycle-14 signature).
	if from, _, perr := state.ParseV4EFBaseName(filepath.Base(newEFPath)); perr == nil && from != baseTxN {
		return fmt.Errorf("TruncateStraddlerHistoryFile: newEFPath %q fromTxN=%d != baseTxN=%d", newEFPath, from, baseTxN)
	}
	if from, _, perr := state.ParseV4VBaseName(filepath.Base(newVPath)); perr == nil && from != baseTxN {
		return fmt.Errorf("TruncateStraddlerHistoryFile: newVPath %q fromTxN=%d != baseTxN=%d", newVPath, from, baseTxN)
	}

	efSrc, err := seg.NewDecompressor(oldEFPath)
	if err != nil {
		return fmt.Errorf("open ef %s: %w", oldEFPath, err)
	}
	defer efSrc.Close()

	vSrc, err := seg.NewDecompressor(oldVPath)
	if err != nil {
		return fmt.Errorf("open v %s: %w", oldVPath, err)
	}
	defer vSrc.Close()

	pageValuesCount := vSrc.CompressedPageValuesCount()
	if pageValuesCount == 0 {
		pageValuesCount = 1
	}

	efReader := seg.NewReader(efSrc.MakeGetter(), efCompression)
	efReader.Reset(0)
	vReader := seg.NewPagedReader(seg.NewReader(vSrc.MakeGetter(), vCompression), pageValuesCount, true)
	vReader.Reset(0)

	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1

	efComp, err := seg.NewCompressor(ctx, "truncate-ef", newEFPath, tmpDir, cfg, log.LvlTrace, logger)
	if err != nil {
		return fmt.Errorf("create ef %s: %w", newEFPath, err)
	}
	var efCompClosed bool
	defer func() {
		if !efCompClosed {
			efComp.Close()
		}
	}()
	efWriter := seg.NewWriter(efComp, efCompression)

	vComp, err := seg.NewCompressor(ctx, "truncate-v", newVPath, tmpDir, cfg.WithValuesOnCompressedPage(pageValuesCount), log.LvlTrace, logger)
	if err != nil {
		return fmt.Errorf("create v %s: %w", newVPath, err)
	}
	var vCompClosed bool
	defer func() {
		if !vCompClosed {
			vComp.Close()
		}
	}()
	vWriter := seg.NewPagedWriter(ctx, seg.NewWriter(vComp, vCompression), pageValuesCount > 0, 1)

	var (
		seqReader multiencseq.SequenceReader
		seqIter   multiencseq.SequenceIterator
		builder   multiencseq.SequenceBuilder

		keyBuf     []byte
		seqBuf     []byte
		valBuf     []byte
		hkBuf      []byte
		outSeqBuf  []byte
		keptTxNums []uint64
		keptValues [][]byte
	)

	for efReader.HasNext() {
		if err = ctx.Err(); err != nil {
			return err
		}
		keyBuf, _ = efReader.Next(keyBuf[:0])
		if !efReader.HasNext() {
			return fmt.Errorf("ef %s: dangling key at eof (no sequence)", oldEFPath)
		}
		seqBuf, _ = efReader.Next(seqBuf[:0])

		seqReader.Reset(baseTxN, seqBuf)
		seqIter.Reset(&seqReader, 0)

		keptTxNums = keptTxNums[:0]
		for i := range keptValues {
			keptValues[i] = nil
		}
		keptValues = keptValues[:0]

		for seqIter.HasNext() {
			txN, iterErr := seqIter.Next()
			if iterErr != nil {
				return fmt.Errorf("ef %s: iterate sequence for key %x: %w", oldEFPath, keyBuf, iterErr)
			}
			if !vReader.HasNext() {
				return fmt.Errorf("v %s: exhausted at ef key=%x txN=%d (ef and v out of sync)", oldVPath, keyBuf, txN)
			}
			valBuf, _ = vReader.Next(valBuf[:0])
			if txN <= targetTxN {
				keptTxNums = append(keptTxNums, txN)
				keptValues = append(keptValues, bytes.Clone(valBuf))
			}
		}

		if len(keptTxNums) == 0 {
			continue
		}

		maxKept := keptTxNums[len(keptTxNums)-1]
		builder.Reset(baseTxN, uint64(len(keptTxNums)), maxKept)
		for _, txN := range keptTxNums {
			builder.AddOffset(txN)
		}
		builder.Build()
		outSeqBuf = builder.AppendBytes(outSeqBuf[:0])

		if _, err = efWriter.Write(keyBuf); err != nil {
			return fmt.Errorf("write ef key %x to %s: %w", keyBuf, newEFPath, err)
		}
		if _, err = efWriter.Write(outSeqBuf); err != nil {
			return fmt.Errorf("write ef sequence for key %x to %s: %w", keyBuf, newEFPath, err)
		}

		for i, txN := range keptTxNums {
			hkBuf = state.HistoryKey(txN, keyBuf, hkBuf)
			if err = vWriter.Add(hkBuf, keptValues[i]); err != nil {
				return fmt.Errorf("write v entry key=%x txN=%d to %s: %w", keyBuf, txN, newVPath, err)
			}
		}
	}

	if err = efWriter.Compress(); err != nil {
		return fmt.Errorf("compress ef %s: %w", newEFPath, err)
	}
	efComp.Close()
	efCompClosed = true

	if err = vWriter.Flush(); err != nil {
		return fmt.Errorf("flush v %s: %w", newVPath, err)
	}
	if err = vWriter.Compress(); err != nil {
		return fmt.Errorf("compress v %s: %w", newVPath, err)
	}
	vComp.Close()
	vCompClosed = true

	return nil
}
