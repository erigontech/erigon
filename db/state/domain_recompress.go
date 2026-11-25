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
)

type RecompressResult struct{ Same, Changed int }

// RecompressDomainFiles writes each .kv file of the domain again with the current compressor.
// A file whose bytes change replaces the original together with its rebuilt accessors, and
// the original goes to snapshots/patch-backup. Files are done one at a time, so the extra
// disk space is one file. The caller must not hold readers of the aggregator's files.
func (a *Aggregator) RecompressDomainFiles(ctx context.Context, name kv.Domain, dryRun bool, logger log.Logger) (RecompressResult, error) {
	at := a.BeginFilesRo()
	ranges := make([][2]uint64, 0, len(at.d[name].files))
	for _, f := range at.d[name].files {
		ranges = append(ranges, [2]uint64{f.startTxNum, f.endTxNum})
	}
	at.Close()

	var res RecompressResult
	for _, r := range ranges {
		changed, err := a.recompressDomainFile(ctx, name, r[0], r[1], dryRun, logger)
		if err != nil {
			return res, err
		}
		if changed {
			res.Changed++
		} else {
			res.Same++
		}
	}
	return res, nil
}

func (a *Aggregator) recompressDomainFile(ctx context.Context, name kv.Domain, fromTxNum, toTxNum uint64, dryRun bool, logger log.Logger) (bool, error) {
	stage, err := a.newPatchStage()
	if err != nil {
		return false, err
	}
	defer dir.RemoveAll(stage) //nolint:errcheck

	staged := map[string]string{}
	at := a.BeginFilesRo()
	dt := at.d[name]
	i := slices.IndexFunc(dt.files, func(f visibleFile) bool { return f.startTxNum == fromTxNum && f.endTxNum == toTxNum })
	if i < 0 {
		at.Close()
		return false, fmt.Errorf("%s file %d-%d is gone", name, fromTxNum, toTxNum)
	}
	original := dt.files[i].Fullpath()
	r, err := rewriteDomainFile(ctx, dt, dt.files[i], nil, stage, staged, logger)
	at.Close()
	if err != nil {
		return false, fmt.Errorf("recompress %s: %w", original, err)
	}

	same, err := sameFileContent(original, a.d[name].kvNewFilePathIn(stage, r.from, r.to))
	if err != nil {
		return false, err
	}
	logger.Info("[recompress]", "file", filepath.Base(original), "changed", !same, "dryRun", dryRun)
	if same || dryRun {
		return !same, nil
	}
	return true, a.replaceFiles(name, []patchedRange{r}, staged, logger)
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
