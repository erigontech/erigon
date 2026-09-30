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

package jsonrpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestResolveWitnessRequest(t *testing.T) {
	stringPtr := func(value string) *string { return &value }
	tests := []struct {
		name        string
		mode        *string
		trie        *string
		binTrie     bool
		wantTrie    witnessTrie
		wantMode    witnessMode
		wantError   string
		wantInvalid bool
	}{
		{name: "omitted mpt", wantTrie: witnessTrieMPT, wantMode: witnessModeLegacy},
		{name: "omitted pbt", binTrie: true, wantTrie: witnessTriePBT, wantMode: witnessModeLegacy},
		{name: "explicit mpt post-fork", trie: stringPtr("mpt"), binTrie: true, wantTrie: witnessTrieMPT, wantMode: witnessModeLegacy},
		{name: "explicit pbt pre-fork", trie: stringPtr("pbt"), wantTrie: witnessTriePBT, wantMode: witnessModeLegacy},
		{name: "pbt with legacy mode", trie: stringPtr("pbt"), mode: stringPtr("legacy"), wantError: "mode applies to the MPT witness only"},
		{name: "pbt with canonical mode", trie: stringPtr("pbt"), mode: stringPtr("canonical"), wantError: "mode applies to the MPT witness only"},
		{name: "unknown trie", trie: stringPtr("binary"), wantInvalid: true},
		{name: "mpt legacy mode", trie: stringPtr("mpt"), mode: stringPtr("legacy"), wantTrie: witnessTrieMPT, wantMode: witnessModeLegacy},
		{name: "mpt canonical after fork", trie: stringPtr("mpt"), mode: stringPtr("canonical"), binTrie: true, wantTrie: witnessTrieMPT, wantMode: witnessModeCanonical},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveWitnessRequest(tt.mode, tt.trie, tt.binTrie)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			if tt.wantInvalid {
				var invalid *rpc.InvalidParamsError
				require.ErrorAs(t, err, &invalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantTrie, got.trie)
			require.Equal(t, tt.wantMode, got.mode)
		})
	}
}

func TestWitnessCacheRoutesByTrie(t *testing.T) {
	api, m := pbinWitnessFixture(t, 30)
	block := rpc.BlockNumber(2)
	var hash common.Hash
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		hash, _, err = m.BlockReader.CanonicalHash(t.Context(), tx, uint64(block))
		return err
	}))
	sentinel := &ExecutionWitnessResult{State: []hexutil.Bytes{{0xde, 0xad}}}
	cache := newWitnessResultCache(96, 0, false, false)
	cache.Add(hash, sentinel)
	api.witnessCache = cache
	t.Cleanup(func() { api.witnessCache = nil })
	tx, err := api.db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	selector := rpc.BlockNumberOrHashWithNumber(block)
	result, hit, reorgedAway := api.serveFromWitnessCache(t.Context(), tx, selector, witnessModeLegacy, witnessTrieMPT, witnessTrieMPT)
	require.True(t, hit)
	require.False(t, reorgedAway)
	require.Same(t, sentinel, result)

	result, hit, reorgedAway = api.serveFromWitnessCache(t.Context(), tx, selector, witnessModeLegacy, witnessTriePBT, witnessTrieMPT)
	require.False(t, hit)
	require.False(t, reorgedAway)
	require.Nil(t, result)
}

func TestExecutionWitnessNonDefaultTrieDoesNotJoinBuild(t *testing.T) {
	api, m := pbinWitnessFixture(t, 30)
	cache := newWitnessResultCache(96, 0, false, false)
	api.witnessCache = cache
	block := uint64(3)
	var hash common.Hash
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		hash, _, err = m.BlockReader.CanonicalHash(t.Context(), tx, block)
		return err
	}))
	started := make(chan struct{})
	release := make(chan struct{})
	buildErr := errors.New("default trie build is still running")
	buildDone := make(chan struct{})
	buildResultErr := make(chan error, 1)
	go func() {
		defer close(buildDone)
		_, err := cache.buildOnce(t.Context(), hash, func() (*ExecutionWitnessResult, error) {
			close(started)
			<-release
			return nil, buildErr
		})
		buildResultErr <- err
	}()
	<-started
	mpt := "mpt"
	requestDone := make(chan struct{})
	var result *ExecutionWitnessResult
	var err error
	go func() {
		result, err = api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(block)), nil, &mpt)
		close(requestDone)
	}()
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		close(release)
		<-buildDone
		t.Fatal("the non-default trie request joined the default-trie build")
	}
	close(release)
	<-buildDone
	require.ErrorIs(t, <-buildResultErr, buildErr)
	require.NoError(t, err)
	require.NotEmpty(t, result.State)
}

func TestWitnessCacheOnlyRejectsNonDefaultTrie(t *testing.T) {
	api, _ := pbinWitnessFixture(t, 30)
	api.witnessCache = newWitnessResultCache(96, 0, true, true)
	t.Cleanup(func() { api.witnessCache = nil })
	block := rpc.BlockNumber(3)
	mpt := "mpt"
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(block), nil, &mpt)
	require.ErrorIs(t, err, errWitnessTrieUnavailable)
	require.Nil(t, result)
}

func TestExecutionWitnessPBTServed(t *testing.T) {
	api, m := pbinWitnessFixture(t, 20)
	repairPBinPreForkShadows(t, m, 20)
	pbt := "pbt"
	mpt := "mpt"
	tests := []struct {
		name  string
		block rpc.BlockNumber
		trie  *string
	}{
		{name: "pre-fork explicit", block: 1, trie: &pbt},
		{name: "first bin block explicit", block: 2, trie: &pbt},
		{name: "first bin block mpt", block: 2, trie: &mpt},
		{name: "post-fork default", block: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(tt.block), nil, tt.trie)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.NotEmpty(t, result.State)
		})
	}
}

func TestExecutionWitnessPBTRefusesHexOnly(t *testing.T) {
	api, _ := pbinWitnessFixture(t, 0, false)
	pbt := "pbt"
	block := rpc.BlockNumber(2)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(block), nil, &pbt)
	require.ErrorContains(t, err, "pbt commitment domain is missing")
	require.Nil(t, result)
}

func TestExecutionWitnessPBTMissingShadowRoot(t *testing.T) {
	api, m := pbinWitnessFixture(t, 20)
	repairPBinPreForkShadows(t, m, 20)
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		header := rawdb.ReadHeaderByNumber(tx, 1)
		return tx.Delete(kv.ShadowStateRoot, dbutils.BlockBodyKey(1, header.Hash()))
	}))
	pbt := "pbt"
	block := rpc.BlockNumber(1)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(block), nil, &pbt)
	require.ErrorContains(t, err, "pbt witness shadow root missing for block 1")
	require.Nil(t, result)
}

func TestExecutionWitnessPBTVerifierRejectsShadowRootMismatch(t *testing.T) {
	api, m := pbinWitnessFixture(t, 20)
	repairPBinPreForkShadows(t, m, 20)
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		header := rawdb.ReadHeaderByNumber(tx, 1)
		badRoot := make([]byte, len(common.Hash{}))
		badRoot[0] = 0x99
		return rawdb.WriteShadowStateRoot(tx, header.Hash(), 1, badRoot)
	}))
	api.witnessCache = newWitnessResultCache(96, 0, false, false)
	pbt := "pbt"
	block := rpc.BlockNumber(1)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(block), nil, &pbt)
	require.Error(t, err)
	require.Nil(t, result)
	require.Empty(t, api.witnessCache.Len())
}

func TestExecutionWitnessMPTAnchorsTransition(t *testing.T) {
	previousAssert := dbg.AssertEnabled
	dbg.AssertEnabled = true
	t.Cleanup(func() { dbg.AssertEnabled = previousAssert })
	api, _ := pbinWitnessFixture(t, 30)
	mpt := "mpt"
	for _, block := range []rpc.BlockNumber{3, 4} {
		t.Run(block.String(), func(t *testing.T) {
			result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(block), nil, &mpt)
			require.NoError(t, err)
			require.NotEmpty(t, result.State)
		})
	}
}

func TestExecutionWitnessMPTMissingShadowRoot(t *testing.T) {
	api, m := pbinWitnessFixture(t, 30)
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		block := rawdb.ReadHeaderByNumber(tx, 4)
		require.NotNil(t, block)
		return tx.Delete(kv.ShadowStateRoot, dbutils.BlockBodyKey(4, block.Hash()))
	}))
	mpt := "mpt"
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(4), nil, &mpt)
	require.ErrorContains(t, err, "mpt")
	require.ErrorContains(t, err, "block 4")
	require.ErrorContains(t, err, "shadow root")
	require.Nil(t, result)
}

func TestExecutionWitnessMPTAvailability(t *testing.T) {
	t.Run("frozen", func(t *testing.T) {
		api, m := pbinWitnessFixture(t, 30)
		tx, err := m.DB.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		txNum, err := m.BlockReader.TxnumReader().Max(t.Context(), tx, 2)
		require.NoError(t, err)
		agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
		require.NoError(t, agg.FreezeDomain(kv.CommitmentDomain, txNum))
		mpt := "mpt"
		result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(3), nil, &mpt)
		require.NoError(t, err)
		require.NotEmpty(t, result.State)
		result, err = api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(4), nil, &mpt)
		require.ErrorContains(t, err, "mpt commitment is frozen")
		require.Nil(t, result)
	})

	t.Run("bin-only", func(t *testing.T) {
		api, _ := pbinWitnessFixture(t, 0)
		mpt := "mpt"
		result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(2), nil, &mpt)
		require.ErrorContains(t, err, "mpt commitment domain is missing")
		require.Nil(t, result)
	})
}

func TestExecutionWitnessPBTAvailability(t *testing.T) {
	t.Run("frozen", func(t *testing.T) {
		api, m := pbinWitnessFixture(t, 20)
		repairPBinPreForkShadows(t, m, 20)
		tx, err := m.DB.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		txNum, err := m.BlockReader.TxnumReader().Max(t.Context(), tx, 2)
		require.NoError(t, err)
		tx.Rollback()
		agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
		require.NoError(t, agg.FreezeDomain(kv.CommitmentBinDomain, txNum))
		pbt := "pbt"
		result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(3), nil, &pbt)
		require.NoError(t, err)
		require.NotEmpty(t, result.State)
		result, err = api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(4), nil, &pbt)
		require.ErrorContains(t, err, "pbt commitment is frozen")
		require.Nil(t, result)
	})

	t.Run("stopped", func(t *testing.T) {
		t.Run("last parent served", func(t *testing.T) {
			api, m := pbinWitnessFixture(t, 20)
			repairPBinPreForkShadows(t, m, 20)
			agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
			require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
				return rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentBinDomain)
			}))
			agg.StopCommitmentDomain(kv.CommitmentBinDomain)
			pbt := "pbt"
			result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(3), nil, &pbt)
			require.NoError(t, err)
			require.NotEmpty(t, result.State)
		})

		t.Run("first parent refused", func(t *testing.T) {
			api, m := pbinWitnessFixture(t, 20)
			repairPBinPreForkShadows(t, m, 20)
			agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
			require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
				for _, table := range m.DB.Debug().DomainTables(kv.CommitmentBinDomain) {
					if err := tx.ClearTable(table); err != nil {
						return err
					}
				}
				return rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentBinDomain)
			}))
			agg.StopCommitmentDomain(kv.CommitmentBinDomain)
			pbt := "pbt"
			result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(4), nil, &pbt)
			require.ErrorContains(t, err, "pbt commitment was stopped before parent")
			require.Nil(t, result)
		})
	})

	t.Run("pruned", func(t *testing.T) {
		api, m := pbinWitnessFixture(t, 20)
		repairPBinPreForkShadows(t, m, 20)
		tx, err := m.DB.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		pruneTo, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, 3)
		require.NoError(t, err)
		cursor, err := tx.RwCursorDupSort(kv.TblCommitmentBinHistoryKeys)
		require.NoError(t, err)
		defer cursor.Close()
		for {
			key, _, err := cursor.First()
			require.NoError(t, err)
			if key == nil || binary.BigEndian.Uint64(key) >= pruneTo {
				break
			}
			require.NoError(t, cursor.DeleteCurrentDuplicates())
		}
		cursor.Close()
		require.NoError(t, tx.Commit())
		pbt := "pbt"
		result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(2), nil, &pbt)
		require.ErrorContains(t, err, "pbt commitment history pruned")
		require.Nil(t, result)
	})
}

func TestExecutionWitnessMPTStoppedAvailability(t *testing.T) {
	previousAssert := dbg.AssertEnabled
	dbg.AssertEnabled = true
	t.Cleanup(func() { dbg.AssertEnabled = previousAssert })
	t.Run("last parent served", func(t *testing.T) {
		api, m := pbinWitnessFixture(t, 30)
		agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
		require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
			return rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentDomain)
		}))
		agg.StopCommitmentDomain(kv.CommitmentDomain)
		mpt := "mpt"
		result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(3), nil, &mpt)
		require.NoError(t, err)
		require.NotEmpty(t, result.State)
	})

	t.Run("first parent refused", func(t *testing.T) {
		api, m := pbinWitnessFixture(t, 30)
		agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
		require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
			for _, table := range m.DB.Debug().DomainTables(kv.CommitmentDomain) {
				if err := tx.ClearTable(table); err != nil {
					return err
				}
			}
			return rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentDomain)
		}))
		agg.StopCommitmentDomain(kv.CommitmentDomain)
		mpt := "mpt"
		result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(4), nil, &mpt)
		require.ErrorContains(t, err, "mpt commitment was stopped before parent")
		require.Nil(t, result)
	})
}

func TestExecutionWitnessMPTStandaloneDatabase(t *testing.T) {
	_, m := pbinWitnessFixture(t, 30)
	primaryDB, ok := m.DB.(*temporal.DB)
	require.True(t, ok)
	primaryRawDB, ok := primaryDB.InternalDB().(*mdbx.MdbxKV)
	require.True(t, ok)
	blockFiles := primaryDB.DebugBlockFiles()
	rawPath := primaryRawDB.Path()
	primaryDB.Close()
	openAPI := func(readonly bool) (*DebugAPIImpl, *temporal.DB) {
		rawDB, err := mdbx.New(dbcfg.ChainDB, log.New()).Readonly(readonly).Path(rawPath).Open(t.Context())
		require.NoError(t, err)
		agg, err := dbstate.NewTest(m.Dirs).Logger(log.New()).Open(t.Context())
		require.NoError(t, err)
		db, err := temporal.New(rawDB, agg, blockFiles)
		require.NoError(t, err)
		api := NewPrivateDebugAPI(NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), db, nil, &rpccfg.DebugApiConfig{})
		return api, db
	}
	mpt := "mpt"
	selector := rpc.BlockNumberOrHashWithNumber(4)
	primaryAPI, primaryDB := openAPI(false)
	primary, err := primaryAPI.ExecutionWitness(t.Context(), selector, nil, &mpt)
	require.NoError(t, err)
	primaryJSON, err := json.Marshal(primary)
	require.NoError(t, err)
	primaryDB.Close()
	standaloneAPI, standaloneDB := openAPI(true)
	standalone, err := standaloneAPI.ExecutionWitness(t.Context(), selector, nil, &mpt)
	require.NoError(t, err)
	standaloneJSON, err := json.Marshal(standalone)
	require.NoError(t, err)
	require.Equal(t, primaryJSON, standaloneJSON)
	standaloneDB.Close()

	primaryAPI, primaryDB = openAPI(false)
	require.NoError(t, primaryDB.Update(t.Context(), func(tx kv.RwTx) error {
		block := rawdb.ReadHeaderByNumber(tx, 4)
		require.NotNil(t, block)
		return tx.Delete(kv.ShadowStateRoot, dbutils.BlockBodyKey(4, block.Hash()))
	}))
	_, primaryErr := primaryAPI.ExecutionWitness(t.Context(), selector, nil, &mpt)
	primaryDB.Close()
	standaloneAPI, standaloneDB = openAPI(true)
	_, standaloneErr := standaloneAPI.ExecutionWitness(t.Context(), selector, nil, &mpt)
	require.EqualError(t, standaloneErr, primaryErr.Error())
	standaloneDB.Close()
}
