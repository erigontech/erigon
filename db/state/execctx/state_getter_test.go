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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
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
	opts       kv.GetLatestOptions
}

func (tx *latestOptionsCaptureTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	metrics, _ := opts.Metrics()
	tx.sawMetrics = metrics != nil
	tx.opts = opts
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

func TestStateGetterUsesSharedBranchCacheForCommitment(t *testing.T) {
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	tx := &latestOptionsCaptureTx{TemporalTx: roTx}
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	branchCache := roTx.AggTx().(commitment.BranchCacheProvider).BranchCache()
	branchCache.Clear()
	getter := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	key := []byte{0xaa, 0xbb}
	cached := []byte("cached")
	branchCache.Put(key, cached, 0, 0)
	tx.opts = kv.GetLatestOptions{}
	got, _, err := getter.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, cached, got)
	branchCache.Clear()
	tx.opts = kv.GetLatestOptions{}
	_, _, err = getter.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.True(t, tx.opts.BranchCache())
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

// codeBufTx stands in for the compressed-file layer: it decodes into the buffer
// the caller lends, the way seg's Getter does.
type codeBufTx struct {
	kv.TemporalTx
	code map[string][]byte
}

func (tx codeBufTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if domain != kv.CodeDomain {
		return tx.TemporalTx.GetLatest(domain, key, opts)
	}
	return append(opts.Buf()[:0], tx.code[string(key)]...), 0, nil
}

func TestStateGetterCodeBufDoesNotAliasEarlierResult(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	codes := [][]byte{
		bytes.Repeat([]byte{0xa1}, 1024),
		bytes.Repeat([]byte{0xb2}, 1024),
		bytes.Repeat([]byte{0xc3}, 1024),
	}
	addrs := make([][]byte, len(codes))
	tx := codeBufTx{TemporalTx: roTx, code: map[string][]byte{}}
	for i := range codes {
		addrs[i] = make([]byte, 20)
		addrs[i][0] = byte(i + 1)
		tx.code[string(addrs[i])] = codes[i]
	}

	sc := newSmallStateCache()
	t.Cleanup(sc.Close)
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.BindStateCache(sc)
	for i, addr := range addrs {
		acc := accounts.SerialiseV3(&accounts.Account{
			Nonce:    1,
			CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(codes[i])),
		})
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, addr, acc, 5, nil))
	}

	getter := sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	got := make([][]byte, len(codes))
	for i, addr := range addrs {
		var ok bool
		got[i], ok, err = getter.GetCode(addr, 5)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, codes[i], got[i])
	}
	for i := range codes {
		require.Equal(t, codes[i], got[i], "read %d must not alias the buffer a later read decodes into", i)
	}
}
