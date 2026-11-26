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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

type RecompressResult struct{ Same, Changed int }

// RecompressDomainFiles writes each .kv and history .v/.ef file of the domain again with the
// current compressor, and skips the file kinds that the domain does not compress. A file whose
// bytes change replaces the original together with its rebuilt accessors, and the original
// goes to <datadir>/patch-backup. Files are done one at a time, so the extra disk space is one
// file. The caller must not hold readers of the aggregator's files.
func (a *Aggregator) RecompressDomainFiles(ctx context.Context, name kv.Domain, dryRun bool, logger log.Logger) (RecompressResult, error) {
	var kvRanges, vRanges []txRange
	at := a.BeginFilesRo()
	if d := a.d[name]; d.DomainCfg.Compression != seg.CompressNone {
		kvRanges = fileRanges(at.d[name].files)
	}
	if h := a.d[name].History; h.Compression != seg.CompressNone {
		vRanges = fileRanges(at.d[name].ht.files)
	}
	at.Close()

	var res RecompressResult
	count := func(changed bool, err error) error {
		if changed {
			res.Changed++
		} else {
			res.Same++
		}
		return err
	}
	for _, r := range kvRanges {
		if err := count(a.recompressFile(ctx, name, r, dryRun, logger, recompressKV)); err != nil {
			return res, err
		}
	}
	for _, r := range vRanges {
		if err := count(a.recompressFile(ctx, name, r, dryRun, logger, recompressHistory)); err != nil {
			return res, err
		}
	}
	return res, nil
}

type txRange struct{ from, to uint64 }

func fileRanges(files visibleFiles) []txRange {
	ranges := make([]txRange, 0, len(files))
	for _, f := range files {
		ranges = append(ranges, txRange{f.startTxNum, f.endTxNum})
	}
	return ranges
}

// recompressor writes the files of range r to stage and returns the original and the new
// path of each data file.
type recompressor func(ctx context.Context, dt *DomainRoTx, r txRange, stage string, staged map[string]string, logger log.Logger) (patchedRange, [][2]string, error)

func recompressKV(ctx context.Context, dt *DomainRoTx, r txRange, stage string, staged map[string]string, logger log.Logger) (patchedRange, [][2]string, error) {
	i := slices.IndexFunc(dt.files, func(f visibleFile) bool { return f.startTxNum == r.from && f.endTxNum == r.to })
	if i < 0 {
		return patchedRange{}, nil, fmt.Errorf("%s .kv %d-%d is gone", dt.d.Name, r.from, r.to)
	}
	pr, err := rewriteDomainFile(ctx, dt, dt.files[i], nil, stage, staged, logger)
	return pr, [][2]string{{dt.files[i].Fullpath(), dt.d.kvNewFilePathIn(stage, pr.from, pr.to)}}, err
}

func recompressHistory(ctx context.Context, dt *DomainRoTx, r txRange, stage string, staged map[string]string, logger log.Logger) (patchedRange, [][2]string, error) {
	vs, efs, err := historyFilePairs(dt)
	if err != nil {
		return patchedRange{}, nil, err
	}
	i := slices.IndexFunc(vs, func(f visibleFile) bool { return f.startTxNum == r.from && f.endTxNum == r.to })
	if i < 0 {
		return patchedRange{}, nil, fmt.Errorf("%s .v %d-%d is gone", dt.d.Name, r.from, r.to)
	}
	pr, err := rewriteHistoryFile(ctx, dt.ht, vs[i], efs[i], nil, stage, staged, logger)
	h := dt.ht.h
	return pr, [][2]string{
		{vs[i].Fullpath(), filepath.Join(stage, filepath.Base(h.vNewFilePath(pr.from, pr.to)))},
		{efs[i].Fullpath(), filepath.Join(stage, filepath.Base(h.InvertedIndex.efNewFilePath(pr.from, pr.to)))},
	}, err
}

func (a *Aggregator) recompressFile(ctx context.Context, name kv.Domain, r txRange, dryRun bool, logger log.Logger, rewrite recompressor) (bool, error) {
	stage, err := a.newPatchStage()
	if err != nil {
		return false, err
	}
	defer dir.RemoveAll(stage) //nolint:errcheck

	staged := map[string]string{}
	at := a.BeginFilesRo()
	pr, files, err := rewrite(ctx, at.d[name], r, stage, staged, logger)
	at.Close()
	if err != nil {
		return false, fmt.Errorf("recompress %s %d-%d: %w", name, r.from, r.to, err)
	}

	changed := false
	for _, f := range files {
		same, err := sameFileContent(f[0], f[1])
		if err != nil {
			return false, err
		}
		logger.Info("[recompress]", "file", filepath.Base(f[0]), "changed", !same, "dryRun", dryRun)
		changed = changed || !same
	}
	if !changed || dryRun {
		return changed, nil
	}
	return true, a.replaceFiles(name, []patchedRange{pr}, staged, logger)
}

func sameFileContent(a, b string) (bool, error) {
	fa, err := os.Open(a)
	if err != nil {
		return false, err
	}
	defer fa.Close()
	fb, err := os.Open(b)
	if err != nil {
		return false, err
	}
	defer fb.Close()
	ra, rb := bufio.NewReaderSize(fa, 1<<20), bufio.NewReaderSize(fb, 1<<20)
	bufA, bufB := make([]byte, 1<<20), make([]byte, 1<<20)
	for {
		na, errA := io.ReadFull(ra, bufA)
		nb, errB := io.ReadFull(rb, bufB)
		if na != nb || !bytes.Equal(bufA[:na], bufB[:nb]) {
			return false, nil
		}
		if errA == io.EOF || errA == io.ErrUnexpectedEOF {
			return errB == io.EOF || errB == io.ErrUnexpectedEOF, nil
		}
		if errA != nil {
			return false, errA
		}
		if errB != nil {
			return false, errB
		}
	}
}
