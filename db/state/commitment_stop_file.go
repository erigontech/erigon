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
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

const COMMITMENT_STOP_FILE = "commitment-stop.toml"

type CommitmentStopPoint struct {
	Block uint64 `toml:"block"`
	TxNum uint64 `toml:"txnum"`
}

func commitmentStopFilePath(dirs datadir.Dirs) string {
	return filepath.Join(dirs.DataDir, COMMITMENT_STOP_FILE)
}

func readCommitmentStopFile(dirs datadir.Dirs) (map[string]CommitmentStopPoint, error) {
	path := commitmentStopFilePath(dirs)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var points map[string]CommitmentStopPoint
	if err := toml.Unmarshal(data, &points); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return points, nil
}

func writeCommitmentStopFile(dirs datadir.Dirs, points map[string]CommitmentStopPoint) error {
	path := commitmentStopFilePath(dirs)
	if len(points) == 0 {
		return dir.RemoveFile(path)
	}
	data, err := toml.Marshal(points)
	if err != nil {
		return err
	}
	return dir.WriteFileAtomic(path, data, 0o644)
}

func WriteCommitmentStopPoint(dirs datadir.Dirs, domain kv.Domain, block, txNum uint64) error {
	points, err := readCommitmentStopFile(dirs)
	if err != nil {
		return err
	}
	if _, ok := points[domain.String()]; ok {
		return nil
	}
	if points == nil {
		points = make(map[string]CommitmentStopPoint, 1)
	}
	points[domain.String()] = CommitmentStopPoint{Block: block, TxNum: txNum}
	return writeCommitmentStopFile(dirs, points)
}

func (a *Aggregator) restoreCommitmentStopFile() error {
	if a.trieVariant != TrieVariantHexBin {
		return nil
	}
	points, err := readCommitmentStopFile(a.dirs)
	if err != nil || len(points) == 0 {
		return err
	}
	var applied, dropped bool
	for _, domain := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		if _, ok := points[domain.String()]; !ok || a.CommitmentDomainStopped(domain) {
			continue
		}
		other := kv.CommitmentDomain
		if domain == kv.CommitmentDomain {
			other = kv.CommitmentBinDomain
		}
		if a.d[domain] == nil || a.d[other] == nil || a.d[other].dirtyFiles.EndTxNumMax() <= a.d[domain].dirtyFiles.EndTxNumMax() {
			delete(points, domain.String())
			dropped = true
			continue
		}
		a.StopCommitmentDomain(domain)
		if a.CanonicalCommitmentDomain() == domain {
			a.SetCanonicalCommitmentDomain(other)
		}
		applied = true
	}
	if applied {
		a.recalcVisibleFiles(nil)
	}
	if dropped {
		return writeCommitmentStopFile(a.dirs, points)
	}
	return nil
}
