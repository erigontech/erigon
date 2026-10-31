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
	"fmt"
	"path/filepath"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

const COMMITMENT_STOP_FILE = "commitment-stop.json"

func commitmentStopFilePath(dirs datadir.Dirs) string {
	return filepath.Join(dirs.Snap, COMMITMENT_STOP_FILE)
}

func readCommitmentStopFile(dirs datadir.Dirs) (map[string]bool, error) {
	path := commitmentStopFilePath(dirs)
	var domains map[string]bool
	found, err := readPBTMarker(path, &domains)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if !found {
		return nil, nil
	}
	return domains, nil
}

func writeCommitmentStopFile(dirs datadir.Dirs, domains map[string]bool) error {
	path := commitmentStopFilePath(dirs)
	if len(domains) == 0 {
		return removePBTMarker(path)
	}
	return writePBTMarker(path, domains)
}

func (a *Aggregator) writeCommitmentStopFile(domain kv.Domain) error {
	a.commitmentStopFileMu.Lock()
	defer a.commitmentStopFileMu.Unlock()
	if a.commitmentStopFileWritten[domain] {
		return nil
	}
	domains, err := readCommitmentStopFile(a.dirs)
	if err != nil {
		return err
	}
	if domains == nil {
		domains = make(map[string]bool, 1)
	}
	if domains[domain.String()] {
		a.commitmentStopFileWritten[domain] = true
		return nil
	}
	domains[domain.String()] = true
	if err := writeCommitmentStopFile(a.dirs, domains); err != nil {
		return err
	}
	a.commitmentStopFileWritten[domain] = true
	return nil
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
		if !points[domain.String()] || a.CommitmentDomainStopped(domain) {
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

func (at *AggregatorRoTx) CommitmentStopFileWriter() func(kv.Domain) error {
	a := at.a
	return a.writeCommitmentStopFile
}
