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

package reset

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func OpenCaplinDB(ctx context.Context, path string, logger log.Logger, readonly bool) (kv.RwDB, error) {
	if _, err := os.Stat(filepath.Join(path, "mdbx.dat")); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	probe, err := mdbx.New(dbcfg.CaplinDB, logger).Path(path).Accede(true).Readonly(true).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{} }).Open(ctx)
	if err != nil {
		return nil, err
	}
	var tables kv.TableCfg
	err = probe.View(ctx, func(tx kv.Tx) error {
		var names []string
		names, err = tx.ListTables()
		if err != nil {
			return err
		}
		tables = make(kv.TableCfg, len(names))
		for _, name := range names {
			if cfg, ok := kv.ChaindataTablesCfg[name]; ok {
				tables[name] = cfg
			}
		}
		return nil
	})
	probe.Close()
	if err != nil {
		return nil, err
	}
	return mdbx.New(dbcfg.CaplinDB, logger).Path(path).Accede(true).Readonly(readonly).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return tables }).Open(ctx)
}
