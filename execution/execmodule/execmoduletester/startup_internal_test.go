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

package execmoduletester

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

type startupTrackingDB struct {
	kv.TemporalRwDB
	closed bool
}

func (db *startupTrackingDB) Close() { db.closed = true }

func TestNilTBStartupTimeoutClosesTester(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	dir := filepath.Join(t.TempDir(), "tester")
	require.NoError(t, os.Mkdir(dir, 0o755))
	db := new(startupTrackingDB)
	m := &ExecModuleTester{Ctx: ctx, Cancel: cancel, DB: db, Dirs: datadir.New(dir), TxPool: new(txpool.TxPool)}
	stopped := make(chan struct{})
	m.bgComponentsEg.Go(func() error {
		defer close(stopped)
		<-ctx.Done()
		return fmt.Errorf("pool startup: %w", ctx.Err())
	})
	t.Cleanup(func() { cancel(); <-stopped })

	var failure any
	func() {
		defer func() { failure = recover() }()
		m.waitForTxPoolStartup(20 * time.Millisecond)
	}()
	err, ok := failure.(error)
	require.True(t, ok, "expected a startup error, got %v", failure)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "transaction pool did not finish startup")
	assert.True(t, db.closed)
	select {
	case <-stopped:
	default:
		t.Error("startup failure left a background task running")
	}
	_, err = os.Stat(dir)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestCloseAcceptsWrappedCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	db := new(startupTrackingDB)
	m := &ExecModuleTester{Ctx: ctx, Cancel: cancel, DB: db, tb: t}
	m.bgComponentsEg.Go(func() error {
		<-ctx.Done()
		return fmt.Errorf("pool startup: %w", ctx.Err())
	})
	m.Close()
	require.True(t, db.closed)
}
