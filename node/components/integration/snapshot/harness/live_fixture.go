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

package harness

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// LoadLiveFixture walks a real snapshots directory (typically a datadir's
// snapshots/ subtree), parses each recognised file with snaptype.ParseFileName,
// and returns a Fixture whose FileEntry slices mirror the on-disk layout.
//
// File content is not read — only sizes and names. A FakePeer seeded from
// this fixture registers synthetic bytes of the declared size with the
// coordinator, which is sufficient for SimulatedTransport-based scenarios.
//
// Directory is walked recursively so datadirs laid out with domain / block
// files in subdirectories (e.g. snapshots/domain, snapshots/idx) are still
// fully inventoried.
func LoadLiveFixture(name, snapshotsDir string) (*Fixture, error) {
	info, err := os.Stat(snapshotsDir)
	if err != nil {
		return nil, fmt.Errorf("live fixture: stat %s: %w", snapshotsDir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("live fixture: %s is not a directory", snapshotsDir)
	}

	f := &Fixture{
		Name:    name,
		Domains: make(map[snapshot.Domain][]*snapshot.FileEntry),
	}

	walkErr := filepath.WalkDir(snapshotsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		dir := filepath.Dir(path)
		parsed, isStateFile, ok := snaptype.ParseFileName(dir, d.Name())
		if !ok {
			return nil
		}

		stat, err := os.Stat(path)
		if err != nil {
			return err
		}

		entry := &snapshot.FileEntry{
			FromStep: parsed.From,
			ToStep:   parsed.To,
			Name:     d.Name(),
			Size:     stat.Size(),
			Local:    true,
			Trust:    snapshot.TrustVerified,
		}
		if isStateFile {
			entry.Domain = snapshot.Domain(parsed.TypeString)
			f.Domains[entry.Domain] = append(f.Domains[entry.Domain], entry)
		} else {
			f.Blocks = append(f.Blocks, entry)
		}
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("live fixture: walk %s: %w", snapshotsDir, walkErr)
	}

	if f.FileCount() == 0 {
		return nil, fmt.Errorf("live fixture: no parseable snapshot files under %s", snapshotsDir)
	}
	return f, nil
}
