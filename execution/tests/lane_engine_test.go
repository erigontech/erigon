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

package executiontests

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/ethconfig"
)

type laneRecorder struct {
	rules.Engine
	err error

	mu    sync.Mutex
	calls map[uint64][]rules.LaneTx
}

type recordedLanes struct{ number uint64 }

func (e *laneRecorder) HasLanes(header *types.Header) bool { return header.Number.Uint64() > 0 }

func (e *laneRecorder) LoadLanes(_ rules.ChainHeaderReader, header *types.Header, _ *state.IntraBlockState) (any, error) {
	return &recordedLanes{number: header.Number.Uint64()}, nil
}

func (e *laneRecorder) InspectTx(tx types.Transaction, ibs *state.IntraBlockState) (any, error) {
	to := tx.GetTo()
	if to == nil || tx.GetValue().IsZero() {
		return false, nil
	}
	hash, err := ibs.GetCodeHash(accounts.InternAddress(*to))
	return !hash.IsEmpty(), err
}

func (e *laneRecorder) VerifyLanes(header *types.Header, lanes any, txs types.Transactions, laneTxs []rules.LaneTx) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	loaded, ok := lanes.(*recordedLanes)
	if !ok || loaded.number != header.Number.Uint64() {
		return fmt.Errorf("block %d: lanes loaded for another block: %v", header.Number.Uint64(), lanes)
	}
	if len(laneTxs) != len(txs) {
		return fmt.Errorf("misaligned lane inputs: %d txs, %d lane txs", len(txs), len(laneTxs))
	}
	e.calls[header.Number.Uint64()] = append([]rules.LaneTx(nil), laneTxs...)
	return e.err
}

// The executor runs InspectTx on the state each transaction starts from: a 7702
// delegation set by an earlier transaction of the same block counts from the next one on.
func TestLaneEngineSeesRecipientCodeBeforeEachTx(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })
	ethconfig.Defaults.Sync.ExecWorkerCount = 4

	authorityKey, err := crypto.ToECDSA(common.LeftPadBytes([]byte{0xa0}, 32))
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
	contract := common.HexToAddress("0xc0c0")
	delegate := common.HexToAddress("0xd0d0")
	absent := common.HexToAddress("0xabab")

	config := chain.TestChainOsakaConfig.Copy()
	config.OsakaTime = nil
	alloc := types.GenesisAlloc{
		contract:  {Code: program.New().Op(vm.STOP).Bytes()},
		delegate:  {Code: program.New().Op(vm.STOP).Bytes()},
		authority: {Balance: big.NewInt(0)},
	}
	keys := revivalKeys(t, alloc, 2)
	sender, setCodeSender := crypto.PubkeyToAddress(keys[0].PublicKey), crypto.PubkeyToAddress(keys[1].PublicKey)
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
	signer := types.LatestSignerForChainID(config.ChainID)
	gasPrice := uint256.NewInt(10_000_000_000)
	auth, err := types.SignAuthorization(authorityKey, *config.ChainID, delegate, 0)
	require.NoError(t, err)

	newTester := func(engine rules.Engine) *execmoduletester.ExecModuleTester {
		return execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]), execmoduletester.WithEngine(engine))
	}
	chainPack, err := newTester(merge.NewFaker(ethash.NewFaker())).GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		send := func(to *common.Address, value uint64) {
			var txn types.Transaction
			if to == nil {
				txn = types.NewContractCreation(b.TxNonce(sender), uint256.NewInt(value), 100_000, gasPrice, []byte{byte(vm.STOP)})
			} else {
				txn = types.NewTransaction(b.TxNonce(sender), *to, uint256.NewInt(value), 100_000, gasPrice, nil)
			}
			signed, err := types.SignTx(txn, *signer, keys[0])
			require.NoError(t, err)
			b.AddTx(signed)
		}
		send(&absent, 1)
		send(&contract, 1)
		send(&contract, 0)
		send(nil, 1)
		send(&authority, 1)
		setCode, err := types.SignTx(&types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: b.TxNonce(setCodeSender), GasLimit: 500_000, To: &authority},
				ChainID:  *config.ChainID,
				TipCap:   *gasPrice,
				FeeCap:   *gasPrice,
			},
			Authorizations: []types.Authorization{auth},
		}, *signer, keys[1])
		require.NoError(t, err)
		b.AddTx(setCode)
		send(&authority, 1)
	})
	require.NoError(t, err)
	wantCode := []bool{false, true, false, false, false, false, true}

	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprintf("parallel=%v", parallel), func(t *testing.T) {
			dbg.Exec3Parallel = parallel
			engine := &laneRecorder{Engine: merge.NewFaker(ethash.NewFaker()), calls: map[uint64][]rules.LaneTx{}}
			require.NoError(t, newTester(engine).InsertChain(chainPack))
			want := make([]rules.LaneTx, len(wantCode))
			for i, receipt := range chainPack.Receipts[0] {
				want[i] = rules.LaneTx{GasUsed: receipt.GasUsed, Info: wantCode[i]}
			}
			require.Equal(t, map[uint64][]rules.LaneTx{1: want}, engine.calls)
		})
		t.Run(fmt.Sprintf("parallel=%v rejects", parallel), func(t *testing.T) {
			dbg.Exec3Parallel = parallel
			violation := errors.New("lane violated")
			engine := &laneRecorder{Engine: merge.NewFaker(ethash.NewFaker()), calls: map[uint64][]rules.LaneTx{}, err: violation}
			err := newTester(engine).InsertChain(chainPack)
			require.ErrorContains(t, err, violation.Error())
		})
	}
}
