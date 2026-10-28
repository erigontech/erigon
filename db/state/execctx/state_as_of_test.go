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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/cache"
)

// headAndBaseTx serves one value per key as the latest state and another as
// the state at asOf, so a read shows which of the two it used.
type headAndBaseTx struct {
	kv.TemporalTx
	head, base map[string][]byte
	asOf       uint64
	asOfReads  []uint64
}

func (tx *headAndBaseTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if domain == kv.CommitmentDomain {
		return tx.TemporalTx.GetLatest(domain, key, opts)
	}
	return tx.head[string(key)], 0, nil
}

func (tx *headAndBaseTx) GetAsOf(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	tx.asOfReads = append(tx.asOfReads, ts)
	if ts != tx.asOf {
		return tx.TemporalTx.GetAsOf(domain, key, ts)
	}
	v, ok := tx.base[string(key)]
	return v, ok, nil
}

func newHeadAndBaseDomains(t *testing.T, head, base map[string][]byte) (*execctx.SharedDomains, *headAndBaseTx) {
	t.Helper()
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(roTx.Rollback)
	tx := &headAndBaseTx{TemporalTx: roTx, head: head, base: base, asOf: 7}
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)
	sd.SetStateAsOf(tx.asOf)
	return sd, tx
}

func TestSetStateAsOfReadsBelowMemAtTxNum(t *testing.T) {
	key := []byte("key")
	sd, tx := newHeadAndBaseDomains(t, map[string][]byte{"key": {5}}, map[string][]byte{"key": {1}})
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		v, _, err := sd.GetLatest(domain, tx, key)
		require.NoError(t, err)
		require.Equal(t, []byte{1}, v, domain.String())
	}
	v, _, err := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}).GetLatest(kv.StorageDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, v)
	require.NotEmpty(t, tx.asOfReads)
	for _, ts := range tx.asOfReads {
		require.Equal(t, tx.asOf, ts)
	}
}

func TestSetStateAsOfReadsCodeSizeAtTxNum(t *testing.T) {
	sd, tx := newHeadAndBaseDomains(t, map[string][]byte{"key": {5, 5, 5}}, map[string][]byte{"key": {1}})
	size, found, err := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}).GetCodeSize([]byte("key"), 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, size)
}

func TestSetStateAsOfKeepsMemWrites(t *testing.T) {
	key := []byte("key")
	sd, tx := newHeadAndBaseDomains(t, map[string][]byte{"key": {5}}, map[string][]byte{"key": {1}})
	require.NoError(t, sd.DomainPut(kv.StorageDomain, tx, key, []byte{9}, 8, nil))
	v, _, err := sd.GetLatest(kv.StorageDomain, tx, key)
	require.NoError(t, err)
	require.Equal(t, []byte{9}, v)
}

func TestSetStateAsOfKeepsPutEqualToLatest(t *testing.T) {
	key := []byte("key")
	sd, tx := newHeadAndBaseDomains(t, map[string][]byte{"key": {5}}, map[string][]byte{})
	require.NoError(t, sd.DomainPut(kv.StorageDomain, tx, key, []byte{5}, 8, nil))
	v, _, ok := sd.GetMemBatch().GetLatest(kv.StorageDomain, key)
	require.True(t, ok)
	require.Equal(t, []byte{5}, v)
}

func TestSetStateAsOfSkipsPutEqualToBase(t *testing.T) {
	key := []byte("key")
	sd, tx := newHeadAndBaseDomains(t, map[string][]byte{"key": {5}}, map[string][]byte{"key": {1}})
	require.NoError(t, sd.DomainPut(kv.StorageDomain, tx, key, []byte{1}, 8, nil))
	_, _, ok := sd.GetMemBatch().GetLatest(kv.StorageDomain, key)
	require.False(t, ok)
}

func TestSetStateAsOfKeepsDeleteOfKeyAbsentAtLatest(t *testing.T) {
	key := []byte("key")
	sd, tx := newHeadAndBaseDomains(t, map[string][]byte{}, map[string][]byte{"key": {1}})
	require.NoError(t, sd.DomainDel(kv.StorageDomain, tx, key, 8, nil))
	v, _, ok := sd.GetMemBatch().GetLatest(kv.StorageDomain, key)
	require.True(t, ok)
	require.Empty(t, v)
	v, _, err := sd.GetLatest(kv.StorageDomain, tx, key)
	require.NoError(t, err)
	require.Empty(t, v)
}

func TestSetStateAsOfLeavesCommitmentLatest(t *testing.T) {
	sd, tx := newHeadAndBaseDomains(t, nil, nil)
	_, _, err := sd.GetLatest(kv.CommitmentDomain, tx, []byte("state"))
	require.NoError(t, err)
	require.Empty(t, tx.asOfReads)
}

func TestSetStateAsOfRejectsStateCache(t *testing.T) {
	sd, _ := newHeadAndBaseDomains(t, nil, nil)
	require.Panics(t, func() { sd.BindStateCache(cache.NewDefaultStateCache()) })
}
