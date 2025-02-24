// Copyright 2024 The Erigon Authors
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

package jsonrpc

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
)

func TestGetChainConfig(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	db, ctx := m.DB, m.Ctx
	api := newBaseApiForTest(m)
	config := m.ChainConfig

	tx, txErr := db.BeginTemporalRo(context.Background())
	if txErr != nil {
		t.Fatalf("error starting tx: %v", txErr)
	}
	defer tx.Rollback()

	config1, err1 := api.chainConfig(ctx, tx)
	if err1 != nil {
		t.Fatalf("reading chain config: %v", err1)
	}
	if config.String() != config1.String() {
		t.Fatalf("read different config: %s, expected %s", config1.String(), config.String())
	}
	config2, err2 := api.chainConfig(ctx, tx)
	if err2 != nil {
		t.Fatalf("reading chain config: %v", err2)
	}
	if config.String() != config2.String() {
		t.Fatalf("read different config: %s, expected %s", config2.String(), config.String())
	}
}
