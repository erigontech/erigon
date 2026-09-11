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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
)

func TestDebugShadowStateRoot(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newDebugApiForTest(m)

	root := common.HexToHash("0x1234")
	require.NoError(t, m.DB.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteShadowStateRoot(tx, m.Genesis.Hash(), 0, root[:])
	}))

	got, err := api.ShadowStateRoot(context.Background(), m.Genesis.Hash())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, root, *got)

	got, err = api.ShadowStateRoot(context.Background(), common.HexToHash("0xbeef"))
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestDebugMigrationProgress(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newDebugApiForTest(m)
	api._genesis.Store(m.Genesis)

	ctx := context.Background()
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	head, err := m.BlockReader.CurrentBlock(tx)
	tx.Rollback()
	require.NoError(t, err)
	require.NotNil(t, head)
	require.Greater(t, head.NumberU64(), uint64(0))

	activation := head.Time() + 1
	cases := []struct {
		mode           string
		binaryTrieTime *uint64
		flipped        bool
	}{
		{mode: dbstate.TrieVariantHex},
		{mode: dbstate.TrieVariantBin, binaryTrieTime: new(uint64), flipped: true},
		{mode: dbstate.TrieVariantHexBin, binaryTrieTime: &activation},
		{mode: dbstate.TrieVariantHexBin, binaryTrieTime: new(uint64), flipped: true},
	}

	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			require.NoError(t, dbstate.WriteErigonDBSettings(m.Dirs, &dbstate.ErigonDBSettings{TrieVariant: &tc.mode}))
			config := m.ChainConfig.Copy()
			config.BinaryTrieTime = tc.binaryTrieTime
			api._chainConfig.Store(config)

			if tc.mode == dbstate.TrieVariantHexBin {
				shadowRoot := common.HexToHash("0x4321")
				require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
					return rawdb.WriteShadowStateRoot(tx, head.Hash(), head.NumberU64(), shadowRoot[:])
				}))
			}

			progress, err := api.MigrationProgress(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.mode, progress.Mode)
			require.Equal(t, tc.flipped, progress.Flipped)
			require.False(t, progress.ShadowStopped)
			if tc.binaryTrieTime == nil {
				require.Nil(t, progress.ActivationTime)
			} else {
				require.NotNil(t, progress.ActivationTime)
				require.Equal(t, *tc.binaryTrieTime, uint64(*progress.ActivationTime))
			}

			if tc.mode == dbstate.TrieVariantHexBin {
				require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
					return tx.Delete(kv.ShadowStateRoot, dbutils.BlockBodyKey(head.NumberU64(), head.Hash()))
				}))

				progress, err = api.MigrationProgress(ctx)
				require.NoError(t, err)
				require.False(t, progress.ShadowStopped)

				shadowDomain, otherDomain := kv.CommitmentBinDomain, kv.CommitmentDomain
				if tc.flipped {
					shadowDomain, otherDomain = otherDomain, shadowDomain
				}
				t.Cleanup(func() {
					require.NoError(t, m.DB.Update(context.Background(), func(tx kv.RwTx) error {
						if err := rawdb.DeleteCommitmentDomainStopped(tx, kv.CommitmentDomain); err != nil {
							return err
						}
						return rawdb.DeleteCommitmentDomainStopped(tx, kv.CommitmentBinDomain)
					}))
				})
				require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
					return rawdb.WriteCommitmentDomainStopped(tx, otherDomain)
				}))
				progress, err = api.MigrationProgress(ctx)
				require.NoError(t, err)
				require.False(t, progress.ShadowStopped)
				require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
					return rawdb.WriteCommitmentDomainStopped(tx, shadowDomain)
				}))
				progress, err = api.MigrationProgress(ctx)
				require.NoError(t, err)
				require.True(t, progress.ShadowStopped)
			}
		})
	}
}
