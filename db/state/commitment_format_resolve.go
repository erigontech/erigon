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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// ResolveCommitmentEdgeRecords picks the commitment record format for a datadir. The format is a
// property of the files already on disk, not of this process: a datadir written with bundled rows
// must keep being read as bundled rows whatever this build defaults to, or every one of its
// commitment files becomes invisible. Only a datadir with no commitment files takes the default.
func ResolveCommitmentEdgeRecords(dirs datadir.Dirs, cfgDefault bool, logger log.Logger) (bool, error) {
	edgeRecords, found, err := commitmentFilesEdgeRecords(dirs)
	if err != nil {
		return false, err
	}
	if !found {
		return cfgDefault, nil
	}
	if edgeRecords != cfgDefault && logger != nil {
		logger.Info("commitment record format taken from existing files",
			"edge_records", edgeRecords, "build_default", cfgDefault)
	}
	return edgeRecords, nil
}

// commitmentFilesEdgeRecords reports the format the datadir's commitment .kv files carry.
// A range holding both is not a datadir this build can read: one file name, one encoding.
func commitmentFilesEdgeRecords(dirs datadir.Dirs) (edgeRecords, found bool, err error) {
	paths, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*.kv"))
	if err != nil {
		return false, false, err
	}
	var firstName string
	for _, path := range paths {
		name := filepath.Base(path)
		v, parseErr := version.ParseVersion(name)
		if parseErr != nil {
			continue
		}
		fileEdgeRecords := statecfg.CommitmentEdgeRecords(v)
		if !found {
			edgeRecords, found, firstName = fileEdgeRecords, true, name
			continue
		}
		if fileEdgeRecords != edgeRecords {
			return false, false, fmt.Errorf("datadir holds both commitment record formats: %s and %s", firstName, name)
		}
	}
	return edgeRecords, found, nil
}
