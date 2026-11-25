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

func ResolveCommitmentV3(ctx context.Context, d datadir.Dirs, chainDB kv.RoDB, logger log.Logger) error {
	if statecfg.CommitmentV3Pinned() {
		return nil
	}
	source, v3, err := commitmentVariantOfFiles(d)
	if err != nil {
		return err
	}
	if source == "" {
		var found bool
		if v3, found, err = chaindataCommitmentVariant(ctx, d, chainDB, logger); err != nil || !found {
			return err
		}
		source = d.Chaindata
	}
	statecfg.SetCommitmentV3(v3)
	logger.Info("[commitment] the datadir selects the commitment trie", "v3", v3, "source", source)
	return nil
}

func commitmentVariantOfFiles(d datadir.Dirs) (source string, v3 bool, err error) {
	for _, dirPath := range []string{d.SnapDomain, d.SnapHistory} {
		legacyName, v3Name, err := commitmentFileNames(dirPath)
		if err != nil {
			return "", false, err
		}
		if v3Name != "" {
			return filepath.Join(dirPath, v3Name), true, nil
		}
		if source == "" && legacyName != "" {
			source = filepath.Join(dirPath, legacyName)
		}
	}
	return source, false, nil
}

func chaindataCommitmentVariant(ctx context.Context, d datadir.Dirs, chainDB kv.RoDB, logger log.Logger) (v3, found bool, err error) {
	if chainDB == nil {
		exists, existErr := dir.FileExist(filepath.Join(d.Chaindata, "mdbx.dat"))
		if existErr != nil || !exists {
			return false, false, existErr
		}
		db, openErr := mdbx.New(dbcfg.ChainDB, logger).Path(d.Chaindata).Accede(true).
			WithTableCfg(func(kv.TableCfg) kv.TableCfg {
				return kv.TableCfg{kv.TblCommitmentVals: {Flags: kv.DupSort}}
			}).Open(ctx)
		if openErr != nil {
			return false, false, fmt.Errorf("opening chaindata to read its commitment state: %w", openErr)
		}
		defer db.Close()
		chainDB = db
	}
	err = chainDB.View(ctx, func(tx kv.Tx) (err error) {
		if v3, err = hasCommitmentKey(tx, commitment.KeyCommitmentV3State); err != nil || v3 {
			found = v3
			return err
		}
		found, err = hasCommitmentKey(tx, commitment.KeyCommitmentState)
		return err
	})
	return v3, found, err
}

func hasCommitmentKey(tx kv.Tx, key []byte) (bool, error) {
	if ttx, ok := tx.(kv.TemporalTx); ok {
		v, _, err := ttx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
		return len(v) > 0, err
	}
	return tx.Has(kv.TblCommitmentVals, key)
}
