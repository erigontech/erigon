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

package rawdb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// TestWriteGenesisIfNotExistMigratesLegacyJSON exercises the in-place rewrite
// path: a stored genesis blob whose ChainID is encoded as the uint256.Int
// default form ("1") should be re-marshalled in the big.Int-compatible form (1)
// the first time WriteGenesisIfNotExist runs on it.
func TestWriteGenesisIfNotExistMigratesLegacyJSON(t *testing.T) {
	t.Parallel()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()

	// Construct a legacy-form blob exactly as a pre-fix Erigon would have
	// written it — by hand, since the current MarshalJSON wouldn't produce it.
	legacy := []byte(`{"config":{"chainName":"mainnet","chainId":"1","terminalTotalDifficulty":"58750000000000000000000"},"nonce":0,"timestamp":0,"extraData":null,"gasLimit":0,"difficulty":"0","mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000","coinbase":"0x0000000000000000000000000000000000000000","alloc":null,"seal":null,"number":0,"gasUsed":0,"parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000","baseFeePerGas":null,"excessBlobGas":null,"blobGasUsed":null}`)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(kv.ConfigTable, kv.GenesisKey, legacy)
	}))

	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteGenesisIfNotExist(tx, nil)
	}))

	var stored []byte
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.ConfigTable, kv.GenesisKey)
		stored = bytes.Clone(v)
		return err
	}))

	// Migrated to unquoted form, readable by *big.Int unmarshalers.
	assert.Contains(t, string(stored), `"chainId":1`)
	assert.Contains(t, string(stored), `"terminalTotalDifficulty":58750000000000000000000`)
	assert.NotContains(t, string(stored), `"chainId":"1"`)

	// Sanity-check: the rewritten blob still round-trips into types.Genesis.
	got, err := rawdb.ReadGenesis(memdbTx(t, db))
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.Config)
	assert.Equal(t, uint64(1), got.Config.ChainID.Uint64())
	require.NotNil(t, got.Config.TerminalTotalDifficulty)
	assert.Equal(t, "58750000000000000000000", got.Config.TerminalTotalDifficulty.Dec())
}

// TestWriteGenesisIfNotExistLeavesCompatBlobAlone verifies the migration is a
// no-op when the stored blob already uses the unquoted form (e.g. on the
// second startup after the fix has run once).
func TestWriteGenesisIfNotExistLeavesCompatBlobAlone(t *testing.T) {
	t.Parallel()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()

	g := &types.Genesis{Config: &chain.Config{ChainName: "mainnet", ChainID: uint256.NewInt(1)}}
	canonical, err := json.Marshal(g)
	require.NoError(t, err)
	require.Contains(t, string(canonical), `"chainId":1`) // sanity

	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(kv.ConfigTable, kv.GenesisKey, canonical)
	}))

	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteGenesisIfNotExist(tx, nil)
	}))

	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.ConfigTable, kv.GenesisKey)
		require.NoError(t, err)
		assert.Equal(t, canonical, v)
		return nil
	}))
}

// memdbTx returns a short-lived RO tx for the calling test. Caller must ensure
// the tx's lifetime fits within the test (it gets rolled back at cleanup).
func memdbTx(t *testing.T, db kv.RwDB) kv.Tx {
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	return tx
}
