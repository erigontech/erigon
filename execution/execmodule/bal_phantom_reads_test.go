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
	"encoding/binary"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

type blockedWarmupDB struct {
	kv.RoDB
	started chan context.Context
}

func (db *blockedWarmupDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	select {
	case db.started <- ctx:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestBALPhantomReadAttackCancelsWarmup(t *testing.T) {
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(true)
	defer dbg.SetReadAhead(oldReadAhead)
	m, block := phantomReadFixture(t, "attack", 2)
	defer m.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	db := &blockedWarmupDB{started: make(chan context.Context, 1)}
	m.ReadAheader.AddBlockAccessList(block.Hash(), block.BlockAccessListSidecar())
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		m.ReadAheader.AddHeaderAndBody(ctx, db, tx, block.Header(), block.Body())
		return nil
	}))
	warmupCtx := <-db.started
	status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	done := make(chan struct{})
	var result execmodule.ValidationResult
	go func() {
		defer close(done)
		result, err = m.ValidateChain(t.Context(), block.Header())
	}()
	select {
	case <-warmupCtx.Done():
	case <-time.After(time.Second):
		t.Error("rejected block left BAL warmup running")
	}
	cancel()
	<-done
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)
	require.Contains(t, result.ValidationError, "block access list mismatch")
	m.ReadAheader.WaitForWarmup(t.Context())
}

func TestBALPhantomReadAttack(t *testing.T) {
	for _, name := range []string{"valid", "reads", "attack"} {
		t.Run(name, func(t *testing.T) {
			m, block := phantomReadFixture(t, name, 2)
			defer m.Close()
			status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			result, err := m.ValidateChain(t.Context(), block.Header())
			require.NoError(t, err)
			if name == "attack" {
				require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)
				require.Contains(t, result.ValidationError, "block access list mismatch")
			} else {
				require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus, result.ValidationError)
			}
		})
	}
}

func phantomReadFixture(tb testing.TB, workload string, workers int) (*execmoduletester.ExecModuleTester, *types.Block) {
	tb.Helper()
	const txCount, phantomSlots = 32, 15_000
	const txGas = uint64(1_000_000)
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(tb, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	burner := common.HexToAddress("0x1234")
	code := program.New()
	const readsPerTx = 400
	if workload == "reads" {
		code.Push(0).Op(vm.CALLDATALOAD)
		for slot := range readsPerTx {
			code.Op(vm.DUP1).Push(slot).Op(vm.ADD, vm.SLOAD, vm.POP)
		}
		code.Op(vm.POP)
	}
	loop := len(code.Bytes())
	code.Op(vm.JUMPDEST).Push(loop).Op(vm.JUMP)
	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges.Copy(),
		GasLimit: uint64(txCount) * txGas,
		Alloc: types.GenesisAlloc{
			sender: {Balance: new(big.Int).Mul(big.NewInt(100), big.NewInt(common.Ether))},
			burner: {Balance: new(big.Int), Code: code.Bytes()},
		},
	}
	m := execmoduletester.New(nil,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithExperimentalBAL(),
		execmoduletester.WithExecWorkerCount(workers),
	)
	pack, err := m.GenerateChain(1, func(_ int, gen *blockgen.BlockGen) {
		for nonce := range uint64(txCount) {
			var data []byte
			if workload == "reads" {
				data = make([]byte, 32)
				binary.BigEndian.PutUint64(data[24:], nonce*readsPerTx)
			}
			tx, err := types.SignTx(types.NewTransaction(nonce, burner, uint256.NewInt(0), txGas,
				uint256.NewInt(m.Genesis.BaseFee().Uint64()*2), data),
				*types.LatestSignerForChainID(genesis.Config.ChainID), key)
			require.NoError(tb, err)
			gen.AddTx(tx)
		}
	})
	require.NoError(tb, err)
	block := pack.TopBlock
	require.Equal(tb, block.GasLimit(), block.GasUsed())
	if workload == "reads" {
		idx := slices.IndexFunc(block.BlockAccessList(), func(ac types.AccountChanges) bool { return ac.Address == burner })
		require.NotEqual(tb, -1, idx)
		require.Equal(tb, readsPerTx*txCount, len(block.BlockAccessList()[idx].StorageReads))
	}
	if workload == "attack" {
		bal := block.BlockAccessList().Copy()
		idx := slices.IndexFunc(bal, func(ac types.AccountChanges) bool { return ac.Address == burner })
		require.NotEqual(tb, -1, idx)
		for slot := range uint64(phantomSlots) {
			var key common.Hash
			binary.BigEndian.PutUint64(key[24:], slot)
			bal[idx].StorageReads = append(bal[idx].StorageReads, key)
		}
		require.NoError(tb, bal.Validate())
		require.NoError(tb, bal.ValidateMaxItems(block.GasLimit()))
		header := block.Header()
		header.BlockAccessListHash = new(bal.Hash())
		block = types.NewBlockFromNetwork(header, block.Body(), types.NewBlockAccessListSidecar(bal))
	}
	return m, block
}
