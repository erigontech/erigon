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
	"context"
	"fmt"
	"path/filepath"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func ResolveCommitmentV3(ctx context.Context, d datadir.Dirs, chainDB kv.RoDB, logger log.Logger) (legacySource string, err error) {
	if statecfg.CommitmentV3Pinned() {
		return "", nil
	}
	legacySource, v3, err := commitmentVariantOfFiles(d)
	if err != nil || v3 {
		return "", err
	}
	if legacySource == "" {
		legacy, err := chaindataHoldsLegacyCommitment(ctx, d, chainDB, logger)
		if err != nil {
			return "", err
		}
		if legacy {
			legacySource = d.Chaindata
		}
	}
	if legacySource == "" {
		return "", nil
	}
	statecfg.SetCommitmentV3(false)
	logger.Info("[commitment] the datadir holds legacy commitment, using the legacy commitment trie", "source", legacySource)
	return legacySource, nil
}

func commitmentVariantOfFiles(d datadir.Dirs) (legacyFile string, v3 bool, err error) {
	for _, dirPath := range []string{d.SnapDomain, d.SnapHistory} {
		legacyName, v3Name, err := commitmentFileNames(dirPath)
		if err != nil {
			return "", false, err
		}
		if v3Name != "" {
			return "", true, nil
		}
		if legacyFile == "" && legacyName != "" {
			legacyFile = filepath.Join(dirPath, legacyName)
		}
	}
	return legacyFile, false, nil
}

func chaindataHoldsLegacyCommitment(ctx context.Context, d datadir.Dirs, chainDB kv.RoDB, logger log.Logger) (bool, error) {
	if chainDB == nil {
		exists, err := dir.FileExist(filepath.Join(d.Chaindata, "mdbx.dat"))
		if err != nil || !exists {
			return false, err
		}
		db, err := mdbx.New(dbcfg.ChainDB, logger).Path(d.Chaindata).Accede(true).
			WithTableCfg(func(kv.TableCfg) kv.TableCfg {
				return kv.TableCfg{kv.TblCommitmentVals: {Flags: kv.DupSort}}
			}).Open(ctx)
		if err != nil {
			return false, fmt.Errorf("opening chaindata to read its commitment state: %w", err)
		}
		defer db.Close()
		chainDB = db
	}
	var legacy bool
	err := chainDB.View(ctx, func(tx kv.Tx) error {
		hasLegacy, err := hasCommitmentKey(tx, commitment.KeyCommitmentState)
		if err != nil || !hasLegacy {
			return err
		}
		hasV3, err := hasCommitmentKey(tx, commitment.KeyCommitmentV3State)
		legacy = !hasV3
		return err
	})
	return legacy, err
}

func hasCommitmentKey(tx kv.Tx, key []byte) (bool, error) {
	if ttx, ok := tx.(kv.TemporalTx); ok {
		v, _, err := ttx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
		return len(v) > 0, err
	}
	return tx.Has(kv.TblCommitmentVals, key)
}
