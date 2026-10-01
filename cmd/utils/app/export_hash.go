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

package app

import (
	"errors"
	"io/fs"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/urfave/cli/v3"
)

func configurePBTExportHash(dirs datadir.Dirs, cliCtx *cli.Command) (func(), error) {
	settings, err := state.ReadErigonDBSettings(dirs)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	variant := state.TrieVariantHex
	if settings != nil {
		variant = settings.TrieVariantName()
	}
	oldBin := statecfg.ExperimentalBinCommitment
	oldHexBin := statecfg.ExperimentalHexBinCommitment
	oldHash := statecfg.BinCommitmentHash
	oldSuite := commitment.PBinHashSuiteName()
	restore := func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.ExperimentalHexBinCommitment = oldHexBin
		statecfg.BinCommitmentHash = oldHash
		_ = commitment.SetPBinHashSuite(oldSuite)
	}
	requested := cliCtx.String("experimental.bin-commitment.hash")
	if variant == state.TrieVariantHex {
		statecfg.ExperimentalBinCommitment = false
		statecfg.ExperimentalHexBinCommitment = false
		statecfg.BinCommitmentHash = ""
		if requested == "" {
			requested = commitment.PBinHashBlake3
		}
	}
	if requested != "" {
		if err := commitment.SetPBinHashSuite(requested); err != nil {
			restore()
			return nil, err
		}
		if variant != state.TrieVariantHex {
			statecfg.BinCommitmentHash = requested
		}
	}
	return restore, nil
}
