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
	"fmt"
	"math"
	"os"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
)

// mergeV4IntoStepFile composes the just-finalized MDBX-only step-aligned
// history collation (at vFinalPath + efFinalPath) with any mode-C paired
// v4 .v / .ef files anchored at step*stepSize (returned by
// h.v4FilesForStep). No-op when no v4 exists.
//
// Contract enforced by mode-C's emit + wipeWritableShadowPast:
//
//   - v4 covers [step*ss, v4EndTxN] entries; MDBX-collation covers
//     (v4EndTxN, (step+1)*ss] entries. The two txN ranges are strictly
//     disjoint, so per (key, txN) there can be at most one source. The
//     merge is a per-key concatenation (v4 txNs first, MDBX txNs second)
//     with no priority resolution needed.
//
// Rewrites vFinalPath + efFinalPath in place. Intermediate step: renames
// the MDBX-only files to a `.mdbx-only` suffix, runs the merge to write
// new files at the final paths, then deletes the intermediates.
//
// For simplicity the first cut supports a single v4 pair per step
// (matching emitSplitStraddler's guarantee of at most one v4 per step).
// Returns an error if multiple v4 items exist.
func (h *History) mergeV4IntoStepFile(ctx context.Context, step kv.Step, vFinalPath, efFinalPath string) (err error) {
	v4VPaths := h.v4FilesForStep(step)
	// TEMP-INSTR-2026-08-23 [dbg-merge-v4] entry log so we can see whether
	// retire's collate finds the v4 for this step. Strip once stage 8f
	// verifies the merge fires end-to-end.
	h.logger.Warn("[dbg-merge-v4] entry", "filenameBase", h.FilenameBase, "step", step, "vFinalPath", vFinalPath, "v4Count", len(v4VPaths), "v4Paths", v4VPaths)
	if len(v4VPaths) == 0 {
		return nil
	}
	if len(v4VPaths) > 1 {
		return fmt.Errorf("mergeV4IntoStepFile: multiple v4 .v items for step %d — not yet supported (paths=%v)", step, v4VPaths)
	}
	v4VPath := v4VPaths[0]
	v4EFPaths := h.InvertedIndex.v4FilesForStep(step)
	if len(v4EFPaths) != 1 {
		return fmt.Errorf("mergeV4IntoStepFile: expected exactly one paired v4 .ef for step %d, found %d (%v)", step, len(v4EFPaths), v4EFPaths)
	}
	v4EFPath := v4EFPaths[0]

	// Rename MDBX-only outputs so we can read them as inputs and write
	// merged output to the final paths.
	mdbxVPath := vFinalPath + ".mdbx-only"
	mdbxEFPath := efFinalPath + ".mdbx-only"
	if err := os.Rename(vFinalPath, mdbxVPath); err != nil {
		return fmt.Errorf("mergeV4IntoStepFile: rename %s → %s: %w", vFinalPath, mdbxVPath, err)
	}
	if err := os.Rename(efFinalPath, mdbxEFPath); err != nil {
		return fmt.Errorf("mergeV4IntoStepFile: rename %s → %s: %w", efFinalPath, mdbxEFPath, err)
	}
	defer func() {
		// Best-effort cleanup of the intermediates. If the merge failed,
		// leaving them behind is fine — the caller will surface the error
		// and a subsequent retire attempt will overwrite them.
		_ = dir.RemoveFile(mdbxVPath)
		_ = dir.RemoveFile(mdbxEFPath)
	}()

	baseTxN := uint64(step) * h.stepSize
	h.logger.Warn("[dbg-merge-v4] merging",
		"filenameBase", h.FilenameBase, "step", step, "baseTxN", baseTxN,
		"v4V", v4VPath, "v4EF", v4EFPath,
		"mdbxV", mdbxVPath, "mdbxEF", mdbxEFPath,
		"outV", vFinalPath, "outEF", efFinalPath)
	if err := h.mergeV4AndMDBXHistoryFiles(ctx, baseTxN, v4EFPath, v4VPath, mdbxEFPath, mdbxVPath, efFinalPath, vFinalPath); err != nil {
		return err
	}
	h.logger.Warn("[dbg-merge-v4] merged OK", "filenameBase", h.FilenameBase, "step", step)
	return nil
}

// mergeV4AndMDBXHistoryFiles is the core merge: two sorted-by-key
// (efPath, vPath) inputs → one sorted-by-key output. Emits per-key
// concatenation of the two sources' txN sequences (v4 first, MDBX
// second, since the txN ranges are disjoint and v4 is earlier).
//
// baseTxN is the shared multiencseq baseNum for all three files (they
// all cover the same step, so all encode against the same base).
func (h *History) mergeV4AndMDBXHistoryFiles(
	ctx context.Context,
	baseTxN uint64,
	v4EFPath, v4VPath string,
	mdbxEFPath, mdbxVPath string,
	outEFPath, outVPath string,
) error {
	// Open source readers.
	v4Cur, err := openHistoryEFVCursor(v4EFPath, v4VPath, h.InvertedIndex.Compression, h.Compression, baseTxN)
	if err != nil {
		return fmt.Errorf("mergeV4AndMDBXHistoryFiles: open v4 pair: %w", err)
	}
	defer v4Cur.Close()

	mdbxCur, err := openHistoryEFVCursor(mdbxEFPath, mdbxVPath, h.InvertedIndex.Compression, h.Compression, baseTxN)
	if err != nil {
		return fmt.Errorf("mergeV4AndMDBXHistoryFiles: open MDBX pair: %w", err)
	}
	defer mdbxCur.Close()

	// Open destination compressors. openHistoryEFVCursor clamps
	// vPageValuesCount to >= 1 for each side, so v4Cur.vPageValuesCount
	// is always usable.
	pageValuesCount := v4Cur.vPageValuesCount

	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1

	efComp, err := seg.NewCompressor(ctx, "merge-v4-ef "+h.FilenameBase, outEFPath, h.dirs.Tmp, cfg, log.LvlTrace, h.logger)
	if err != nil {
		return fmt.Errorf("open ef compressor %s: %w", outEFPath, err)
	}
	var efCompClosed bool
	defer func() {
		if !efCompClosed {
			efComp.Close()
		}
	}()
	efWriter := seg.NewWriter(efComp, h.InvertedIndex.Compression)

	vComp, err := seg.NewCompressor(ctx, "merge-v4-v "+h.FilenameBase, outVPath, h.dirs.Tmp, cfg.WithValuesOnCompressedPage(pageValuesCount), log.LvlTrace, h.logger)
	if err != nil {
		return fmt.Errorf("open v compressor %s: %w", outVPath, err)
	}
	var vCompClosed bool
	defer func() {
		if !vCompClosed {
			vComp.Close()
		}
	}()
	vWriter := seg.NewPagedWriter(ctx, seg.NewWriter(vComp, h.Compression), pageValuesCount > 0, 1)

	var (
		builder multiencseq.SequenceBuilder
		hkBuf   []byte
		seqBuf  []byte

		mergedTxNs   []uint64
		mergedValues [][]byte
	)

	for v4Cur.hasKey || mdbxCur.hasKey {
		if err := ctx.Err(); err != nil {
			return err
		}

		var pickV4, pickMDBX bool
		var outKey []byte
		switch {
		case !v4Cur.hasKey:
			pickMDBX = true
			outKey = mdbxCur.key
		case !mdbxCur.hasKey:
			pickV4 = true
			outKey = v4Cur.key
		default:
			cmp := bytes.Compare(v4Cur.key, mdbxCur.key)
			switch {
			case cmp < 0:
				pickV4 = true
				outKey = v4Cur.key
			case cmp > 0:
				pickMDBX = true
				outKey = mdbxCur.key
			default:
				pickV4 = true
				pickMDBX = true
				outKey = v4Cur.key
			}
		}

		mergedTxNs = mergedTxNs[:0]
		for i := range mergedValues {
			mergedValues[i] = nil
		}
		mergedValues = mergedValues[:0]

		// v4/MDBX overlap semantics: WipeWritableShadowPast only prunes
		// history entries at txN > lastTxN, so MDBX post-wipe retains all
		// (key, txN) tuples for txN <= lastTxN — the exact range v4
		// covers. When both sides carry the same key AND MDBX's
		// smallest txN falls inside v4's range, v4's overlapping tail is
		// redundant (its values were originally copied FROM the same
		// MDBX at emit time). Drop v4 entries at or past MDBX's first
		// txN so the merged sequence stays strictly ascending without
		// duplicates. Non-overlap case (mdbx.min > v4.max) keeps every
		// v4 entry unchanged.
		mdbxCutoff := uint64(math.MaxUint64)
		if pickV4 && pickMDBX && len(mdbxCur.txNs) > 0 {
			mdbxCutoff = mdbxCur.txNs[0]
		}
		if pickV4 {
			for i, txN := range v4Cur.txNs {
				if txN >= mdbxCutoff {
					break
				}
				mergedTxNs = append(mergedTxNs, txN)
				mergedValues = append(mergedValues, v4Cur.values[i])
			}
		}
		if pickMDBX {
			mergedTxNs = append(mergedTxNs, mdbxCur.txNs...)
			mergedValues = append(mergedValues, mdbxCur.values...)
		}

		if len(mergedTxNs) == 0 {
			return fmt.Errorf("mergeV4AndMDBXHistoryFiles: empty sequence for key %x (should not happen)", outKey)
		}

		maxTxN := mergedTxNs[len(mergedTxNs)-1]
		builder.Reset(baseTxN, uint64(len(mergedTxNs)), maxTxN)
		for _, txN := range mergedTxNs {
			builder.AddOffset(txN)
		}
		builder.Build()
		seqBuf = builder.AppendBytes(seqBuf[:0])

		if _, err := efWriter.Write(outKey); err != nil {
			return fmt.Errorf("write ef key %x: %w", outKey, err)
		}
		if _, err := efWriter.Write(seqBuf); err != nil {
			return fmt.Errorf("write ef sequence for key %x: %w", outKey, err)
		}

		for i, txN := range mergedTxNs {
			hkBuf = historyKey(txN, outKey, hkBuf)
			if err := vWriter.Add(hkBuf, mergedValues[i]); err != nil {
				return fmt.Errorf("write v entry key=%x txN=%d: %w", outKey, txN, err)
			}
		}

		if pickV4 {
			if err := v4Cur.advance(); err != nil {
				return fmt.Errorf("advance v4 cursor: %w", err)
			}
		}
		if pickMDBX {
			if err := mdbxCur.advance(); err != nil {
				return fmt.Errorf("advance MDBX cursor: %w", err)
			}
		}
	}

	if err := efWriter.Compress(); err != nil {
		return fmt.Errorf("compress ef %s: %w", outEFPath, err)
	}
	efComp.Close()
	efCompClosed = true

	if err := vWriter.Flush(); err != nil {
		return fmt.Errorf("flush v %s: %w", outVPath, err)
	}
	if err := vWriter.Compress(); err != nil {
		return fmt.Errorf("compress v %s: %w", outVPath, err)
	}
	vComp.Close()
	vCompClosed = true

	return nil
}

// historyEFVCursor pairs an .ef reader with the .v paged reader that
// holds its values, decoding one (key, txNs, values) tuple at a time
// in the file's ascending-key order. Used by mergeV4AndMDBXHistoryFiles
// as a common input abstraction over v4 and MDBX-collated pairs.
type historyEFVCursor struct {
	efDec *seg.Decompressor
	vDec  *seg.Decompressor

	efReader *seg.Reader
	vReader  *seg.PagedReader

	baseTxN          uint64
	vPageValuesCount int

	seqReader multiencseq.SequenceReader
	seqIter   multiencseq.SequenceIterator

	// current tuple
	hasKey bool
	key    []byte
	txNs   []uint64
	values [][]byte
}

func openHistoryEFVCursor(efPath, vPath string, efCompression, vCompression seg.FileCompression, baseTxN uint64) (*historyEFVCursor, error) {
	efDec, err := seg.NewDecompressor(efPath)
	if err != nil {
		return nil, fmt.Errorf("open ef %s: %w", efPath, err)
	}
	vDec, err := seg.NewDecompressor(vPath)
	if err != nil {
		efDec.Close()
		return nil, fmt.Errorf("open v %s: %w", vPath, err)
	}
	pageValuesCount := vDec.CompressedPageValuesCount()
	if pageValuesCount == 0 {
		pageValuesCount = 1
	}
	c := &historyEFVCursor{
		efDec:            efDec,
		vDec:             vDec,
		efReader:         seg.NewReader(efDec.MakeGetter(), efCompression),
		vReader:          seg.NewPagedReader(seg.NewReader(vDec.MakeGetter(), vCompression), pageValuesCount, true),
		baseTxN:          baseTxN,
		vPageValuesCount: pageValuesCount,
	}
	c.efReader.Reset(0)
	c.vReader.Reset(0)
	if err := c.advance(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *historyEFVCursor) advance() error {
	if !c.efReader.HasNext() {
		c.hasKey = false
		c.key = nil
		c.txNs = c.txNs[:0]
		for i := range c.values {
			c.values[i] = nil
		}
		c.values = c.values[:0]
		return nil
	}
	c.key, _ = c.efReader.Next(c.key[:0])
	if !c.efReader.HasNext() {
		return fmt.Errorf("historyEFVCursor: dangling key %x at eof", c.key)
	}
	seqBytes, _ := c.efReader.Next(nil)

	c.seqReader.Reset(c.baseTxN, seqBytes)
	c.seqIter.Reset(&c.seqReader, 0)

	c.txNs = c.txNs[:0]
	for i := range c.values {
		c.values[i] = nil
	}
	c.values = c.values[:0]

	for c.seqIter.HasNext() {
		txN, err := c.seqIter.Next()
		if err != nil {
			return fmt.Errorf("iterate sequence for key %x: %w", c.key, err)
		}
		if !c.vReader.HasNext() {
			return fmt.Errorf("v exhausted at key=%x txN=%d (ef/v out of sync)", c.key, txN)
		}
		val, _ := c.vReader.Next(nil)
		c.txNs = append(c.txNs, txN)
		c.values = append(c.values, bytes.Clone(val))
	}
	c.hasKey = true
	return nil
}

func (c *historyEFVCursor) Close() {
	if c.efDec != nil {
		c.efDec.Close()
		c.efDec = nil
	}
	if c.vDec != nil {
		c.vDec.Close()
		c.vDec = nil
	}
}
