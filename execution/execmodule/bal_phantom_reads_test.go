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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
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

type warmupVerdictObserver struct {
	log.Handler
	warmupCtx context.Context
	atVerdict chan error
}

func (h *warmupVerdictObserver) Enabled(context.Context, log.Lvl) bool { return true }

func (h *warmupVerdictObserver) Log(record *log.Record) error {
	if record.Msg == "ethereumExecutionModule.ValidateChain: chain is invalid" {
		// Observe cancellation before ValidateChain runs its deferred cleanup.
		select {
		case h.atVerdict <- h.warmupCtx.Err():
		default:
		}
	}
	return h.Handler.Log(record)
}

func TestValidateChainCancelsWarmup(t *testing.T) {
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(true)
	defer dbg.SetReadAhead(oldReadAhead)
	oldReadAheadWait := dbg.ReadAheadWait
	dbg.ReadAheadWait = false
	defer func() { dbg.ReadAheadWait = oldReadAheadWait }()
	for _, tc := range []struct {
		name      string
		workload  string
		wantError string
	}{
		{"valid", "valid", ""},
		{"BAL mismatch", "attack", "block access list mismatch"},
		{"wrong trie root", "valid", "wrong trie root"},
		{"wrong receipts", "valid", "receiptHash mismatch"},
		{"invalid transaction", "valid", "nonce too high"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, block := phantomReadFixture(t, tc.workload, 2)
			header := block.Header()
			body := block.Body()
			switch tc.name {
			case "wrong trie root":
				header.Root[0] ^= 1
			case "wrong receipts":
				header.ReceiptHash[0] ^= 1
			case "invalid transaction":
				tx := body.Transactions[0]
				invalidTx, err := types.SignTx(types.NewTransaction(tx.GetNonce()+1, *tx.GetTo(), tx.GetValue(), tx.GetGasLimit(), tx.GetFeeCap(), tx.GetData()),
					*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
				require.NoError(t, err)
				body.Transactions = slices.Clone(body.Transactions)
				body.Transactions[0] = invalidTx
				header.TxHash = types.DeriveSha(types.Transactions(body.Transactions))
			}
			block = types.NewBlockFromNetwork(header, body, block.BlockAccessListSidecar())
			ctx, cancel := context.WithCancel(t.Context())
			defer func() {
				cancel()
				waitCtx, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
				defer stopWaiting()
				m.ReadAheader.WaitForWarmup(waitCtx)
				assert.NoError(t, waitCtx.Err(), "warmup did not stop after cancellation")
			}()
			db := &blockedWarmupDB{started: make(chan context.Context, 1)}
			m.ReadAheader.AddBlockAccessList(block.Hash(), block.BlockAccessListSidecar())
			require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
				m.ReadAheader.AddHeaderAndBody(ctx, db, tx, block.Header(), block.Body())
				return nil
			}))
			var warmupCtx context.Context
			select {
			case warmupCtx = <-db.started:
			case <-time.After(5 * time.Second):
				t.Fatal("warmup did not start")
			}
			observer := &warmupVerdictObserver{
				Handler: m.Log.GetHandler(), warmupCtx: warmupCtx, atVerdict: make(chan error, 1),
			}
			m.Log.SetHandler(observer)
			defer m.Log.SetHandler(observer.Handler)
			status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			done := make(chan struct{})
			var result execmodule.ValidationResult
			require.NoError(t, warmupCtx.Err(), "warmup must remain active before validation")
			go func() {
				defer close(done)
				result, err = m.ValidateChain(t.Context(), block.Header())
			}()
			select {
			case <-done:
				assert.ErrorIs(t, warmupCtx.Err(), context.Canceled, "validation returned with warmup still active")
			case <-time.After(time.Minute):
				cancel()
				m.Cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("block validation did not stop after cancellation")
				}
				t.Fatal("block validation did not finish within one minute")
			}
			require.NoError(t, err)
			if tc.wantError == "" {
				require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus, result.ValidationError)
			} else {
				require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus, result.ValidationError)
				require.Contains(t, result.ValidationError, tc.wantError)
				select {
				case err := <-observer.atVerdict:
					require.ErrorIs(t, err, context.Canceled, "validation reported an invalid block with warmup still active")
				default:
					t.Fatal("validation did not report the invalid block")
				}
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
	_, loop := code.Jumpdest()
	code.Jump(loop)
	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges.Copy(),
		GasLimit: uint64(txCount) * txGas,
		Alloc: types.GenesisAlloc{
			sender: {Balance: new(big.Int).Mul(big.NewInt(100), big.NewInt(common.Ether))},
			burner: {Balance: new(big.Int), Code: code.Bytes()},
		},
	}
	m := execmoduletester.New(tb,
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
