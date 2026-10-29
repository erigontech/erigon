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

package tools

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
)

// MirrorEntry is a line of a mirror index: a batch the rollup settled and
// the versioned hashes of its posting transaction's blobs.
type MirrorEntry struct {
	L1Block uint64        `json:"l1Block"`
	L1Hash  common.Hash   `json:"l1Hash"`
	L1Time  uint64        `json:"l1Time"`
	Tx      common.Hash   `json:"tx"`
	Blobs   []common.Hash `json:"blobs"`
}

// ReadIndex reads the mirror index at path, one JSON entry per line in L1
// order; a missing file is an empty index.
func ReadIndex(path string) ([]MirrorEntry, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []MirrorEntry
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		var e MirrorEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		if n := len(out); n > 0 && e.L1Block < out[n-1].L1Block {
			return nil, fmt.Errorf("%s:%d: L1 block %d after %d", path, line, e.L1Block, out[n-1].L1Block)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// MirrorStats reports a Mirror run.
type MirrorStats struct {
	// Indexed is the number of batches appended to the index.
	Indexed int
	// Stored is the number of blobs written to the directory.
	Stored int
	// Unavailable lists the indexed batches with a blob neither the
	// directory nor its source holds.
	Unavailable []MirrorEntry
}

// mirrorAttempts bounds the reads of one batch's blobs that fail for other
// reasons than the source not holding them, such as rate limits.
const mirrorAttempts = 6

// Mirror appends to the index at path the batches scanner finds after the
// last indexed one, or from from when the index is empty, through to. It
// then stores in dir every indexed blob dir lacks, read through dir from
// its source.
func Mirror(ctx context.Context, scanner *l1.Scanner, dir *l1.BlobDir, path string, from, to uint64, logger log.Logger) (MirrorStats, error) {
	var stats MirrorStats
	entries, err := ReadIndex(path)
	if err != nil {
		return stats, err
	}
	if n := len(entries); n > 0 {
		from = entries[n-1].L1Block + 1
	}
	if from <= to {
		if entries, stats.Indexed, err = appendIndex(ctx, scanner, path, entries, from, to); err != nil {
			return stats, err
		}
	}
	for _, e := range entries {
		var need []common.Hash
		for _, h := range e.Blobs {
			ok, err := dir.Has(h)
			if err != nil {
				return stats, err
			}
			if !ok {
				need = append(need, h)
			}
		}
		if len(need) == 0 {
			continue
		}
		header := &l1.Header{Number: e.L1Block, Hash: e.L1Hash, Time: e.L1Time}
		for attempt := 1; ; attempt++ {
			_, err = dir.Blobs(ctx, header, need)
			if err == nil || errors.Is(err, l1.ErrBlobUnavailable) || attempt == mirrorAttempts {
				break
			}
			select {
			case <-ctx.Done():
				return stats, ctx.Err()
			case <-time.After(time.Second << attempt):
			}
		}
		switch {
		case errors.Is(err, l1.ErrBlobUnavailable):
			stats.Unavailable = append(stats.Unavailable, e)
		case err != nil:
			return stats, fmt.Errorf("blobs of L1 block %d: %w", e.L1Block, err)
		default:
			stats.Stored += len(need)
			if stats.Stored%500 < len(need) {
				logger.Info("[eez] mirror", "stored", stats.Stored, "l1Block", e.L1Block)
			}
		}
	}
	return stats, nil
}

// appendIndex appends to the index at path the batches of [from, to], each
// scanned range in one write.
func appendIndex(ctx context.Context, scanner *l1.Scanner, path string, entries []MirrorEntry, from, to uint64) ([]MirrorEntry, int, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, 0, err
	}
	indexed := 0
	for from <= to {
		batches, reached, err := scanner.Scan(ctx, from, to)
		if err != nil {
			f.Close()
			return nil, 0, err
		}
		var buf []byte
		for i := range batches {
			b := &batches[i]
			e := MirrorEntry{L1Block: b.L1Block, L1Hash: b.L1BlockHash, L1Time: b.L1Time, Tx: b.TxHash, Blobs: b.BlobHashes}
			line, err := json.Marshal(e)
			if err != nil {
				f.Close()
				return nil, 0, err
			}
			buf = append(append(buf, line...), '\n')
			entries = append(entries, e)
		}
		if _, err := f.Write(buf); err != nil {
			f.Close()
			return nil, 0, err
		}
		indexed += len(batches)
		from = reached + 1
	}
	return entries, indexed, f.Close()
}
