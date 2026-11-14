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

package execmodule_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestExecModuleProducesE2Snapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	retireStep := uint64(10)
	chainLen := int(retireStep) + 2
	emt := execmoduletester.New(
		t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithE2RetireStep(retireStep),
		execmoduletester.WithMaxReorgDepth(1),
	)
	require.NoError(t, emt.WaitForBlockRetirement(ctx))
	cp, err := emt.GenerateChain(chainLen, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(
				gen.TxNonce(emt.Address),
				common.Address{1},
				uint256.NewInt(10_000),
				params.TxGas,
				uint256.NewInt(emt.Genesis.BaseFee().Uint64()),
				nil,
			),
			*types.LatestSignerForChainID(emt.ChainConfig.ChainID),
			emt.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	status, err := emt.InsertBlocks(ctx, cp.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := emt.UpdateForkChoice(ctx, cp.Blocks[len(cp.Blocks)-2].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	require.NoError(t, emt.WaitForBlockRetirement(ctx))
	result, err = emt.UpdateForkChoice(ctx, cp.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	require.NoError(t, emt.WaitForBlockRetirement(ctx))
	err = emt.BlockSnapshots.OpenFolder()
	require.NoError(t, err)
	require.Equal(t, uint64(9), emt.BlockSnapshots.BlocksAvailable())
}

// Block retire, state files, a reorg and reads over all of them must not keep a deleted file of the datadir
// open or mapped: its disk space would stay used until restart.
func TestExecModuleLeavesNoDeletedFileOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	const (
		retireStep = 10
		chainLen   = 3*retireStep + 2
		reorgFrom  = chainLen - 2
	)
	emt := execmoduletester.New(
		t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithE2RetireStep(retireStep),
		execmoduletester.WithStepSize(3),
		execmoduletester.WithMaxReorgDepth(4),
	)
	datadir, err := filepath.EvalSymlinks(emt.Dirs.DataDir)
	require.NoError(t, err)
	if _, err := dir.DeletedOpenFiles(datadir); errors.Is(err, errors.ErrUnsupported) {
		return // no /proc/self/fd on this platform
	}
	require.NoError(t, emt.WaitForBlockRetirement(ctx))
	require.NoError(t, emt.WaitForStateRetirement(ctx))
	generate := func(to func(i int) common.Address) *blockgen.ChainPack {
		cp, err := emt.GenerateChain(chainLen, func(i int, gen *blockgen.BlockGen) {
			tx, err := types.SignTx(
				types.NewTransaction(gen.TxNonce(emt.Address), to(i), uint256.NewInt(10_000), params.TxGas, uint256.NewInt(emt.Genesis.BaseFee().Uint64()), nil),
				*types.LatestSignerForChainID(emt.ChainConfig.ChainID),
				emt.Key,
			)
			require.NoError(t, err)
			gen.AddTx(tx)
		})
		require.NoError(t, err)
		return cp
	}
	cp1 := generate(func(int) common.Address { return common.Address{1} })
	cp2 := generate(func(i int) common.Address {
		if i+1 < reorgFrom {
			return common.Address{1}
		}
		return common.Address{2}
	})
	require.NoError(t, emt.InsertValidateAndUfc1By1(ctx, cp1.Blocks, execmoduletester.WithWaitForBlockRetirement(), execmoduletester.WithWaitForStateFiles()))

	status, err := emt.InsertBlocks(ctx, cp2.Blocks[reorgFrom-1:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := emt.UpdateForkChoice(ctx, cp2.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	require.NoError(t, emt.WaitForBlockRetirement(ctx))

	readAll := func() {
		tx, err := emt.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		for n := range uint64(chainLen + 1) {
			_, err := emt.BlockReader.BlockByNumber(ctx, tx, n)
			require.NoError(t, err)
		}
		for ts := range uint64(4 * chainLen) {
			_, _, err := tx.GetAsOf(kv.AccountsDomain, emt.Address[:], ts)
			require.NoError(t, err)
		}
	}
	readAll()

	var mapped, deleted []string
	for range 100 { // reclaim of retired files can still be running
		mapped, _, err = mmap.DeletedMappings(datadir)
		require.NoError(t, err)
		deleted, err = dir.DeletedOpenFiles(datadir)
		require.NoError(t, err)
		if len(mapped) == 0 && len(deleted) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Empty(t, mapped, "deleted files are still mapped")
	require.Empty(t, deleted, "deleted files are still open")
}
