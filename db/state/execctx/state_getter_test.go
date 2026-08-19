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

package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
)

type valSizeTx struct {
	kv.TemporalTx
	size  int
	found bool
}

func (tx valSizeTx) GetLatestValSize(kv.Domain, []byte) (int, bool, error) {
	return tx.size, tx.found, nil
}

type latestOptionsCaptureTx struct {
	kv.TemporalTx
	sawMetrics bool
}

func (tx *latestOptionsCaptureTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	metrics, _ := opts.Metrics()
	tx.sawMetrics = metrics != nil
	return tx.TemporalTx.GetLatest(domain, key, opts)
}

func TestPlainGetLatestDoesNotPassNilRequestMetrics(t *testing.T) {
	metricsEnabled := dbg.KVReadLevelledMetrics
	dbg.KVReadLevelledMetrics = true
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = metricsEnabled })
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	tx := &latestOptionsCaptureTx{TemporalTx: roTx}
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	tx.sawMetrics = false
	key := make([]byte, 20)
	_, _, err = sd.GetLatest(kv.AccountsDomain, tx, key)
	require.NoError(t, err)
	require.False(t, tx.sawMetrics)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, key, []byte{1}, 1, nil))
	_, _, err = sd.GetLatest(kv.AccountsDomain, tx, key)
	require.NoError(t, err)
}

func TestTemporalTxStateGetterCodePresence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		size      int
		keyFound  bool
		codeFound bool
	}{
		{name: "missing", keyFound: false, codeFound: false},
		{name: "empty record", keyFound: true, codeFound: false},
		{name: "code", size: 3, keyFound: true, codeFound: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			getter := execctx.NewTemporalTxStateGetter(valSizeTx{size: test.size, found: test.keyFound})
			size, found, err := getter.GetCodeSize(nil, 0)
			require.NoError(t, err)
			require.Equal(t, test.size, size)
			require.Equal(t, test.codeFound, found)
		})
	}
}
