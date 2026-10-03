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

package commands

import (
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
)

func openPBTState(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, rawDB kv.RwDB, logger log.Logger) (*dbstate.Aggregator, error) {
	agg, err := dbstate.NewPBTStateAggregator(dirs, settings, logger).Open(ctx)
	if err != nil {
		return nil, err
	}
	if err := agg.OpenFolder(rawDB); err != nil {
		agg.Close()
		return nil, err
	}
	return agg, nil
}
