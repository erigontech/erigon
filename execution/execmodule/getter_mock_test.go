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

package execmodule

import (
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
)

// NewGetterMockForTest builds a synchronous ExecModule that serves the
// payload-bodies getters straight from the raw DB. It publishes no
// SharedDomains, so beginOverlayOrRo reads the committed database directly, and
// starts no background-commit worker — tests set DB state themselves and read
// it back deterministically. Exported from a _test.go file: usable by the
// execmodule_test binary, never compiled into a production build.
func NewGetterMockForTest(db kv.TemporalRwDB, blockReader dbservices.FullBlockReader, engine rules.Engine, config *chain.Config, logger log.Logger) *ExecModule {
	return &ExecModule{
		db:             db,
		blockReader:    blockReader,
		balRegenerator: bal.NewRegenerator(blockReader, engine, logger),
		config:         config,
		logger:         logger,
	}
}
