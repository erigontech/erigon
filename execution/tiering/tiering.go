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

package tiering

import (
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/db/kv"
)

// ColdBoundary returns the first block of the hot window of the last lookback blocks before head.
func ColdBoundary(head, lookback uint64) uint64 {
	if head < lookback {
		return 0
	}
	return head - lookback
}

// ColdFiles returns the files that hold only txnums below boundaryTxNum.
func ColdFiles(files kv.VisibleFiles, boundaryTxNum uint64) kv.VisibleFiles {
	var cold kv.VisibleFiles
	for _, f := range files {
		if f.EndRootNum() <= boundaryTxNum {
			cold = append(cold, f)
		}
	}
	return cold
}

// File is a visible state file: its txnum range [Start, End), its size and its symlink-resolved path.
type File struct {
	Start, End, Size uint64
	Path             string
}

type Usage struct {
	Files int
	Bytes uint64
}

// Lag splits the files that should be on the cold tier but are not, by reason.
type Lag struct {
	ColdResident, CopyPending, Straddling Usage
}

func ComputeLag(files []File, boundaryTxNum uint64, coldDir string) Lag {
	var lag Lag
	for _, f := range files {
		var u *Usage
		switch {
		case coldDir != "" && strings.HasPrefix(f.Path, filepath.Clean(coldDir)+string(filepath.Separator)):
			u = &lag.ColdResident
		case f.End <= boundaryTxNum:
			u = &lag.CopyPending
		case f.Start < boundaryTxNum:
			u = &lag.Straddling
		default:
			continue
		}
		u.Files++
		u.Bytes += f.Size
	}
	return lag
}
