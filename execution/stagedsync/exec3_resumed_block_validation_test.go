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

package stagedsync

import (
	"bytes"
	"context"
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// An Amsterdam block with two txs from the coinbase: a transfer to 0x0a11 and
// a call to 0x0b22, which stores the balance of 0x0a11.
const (
	resumedBlockRLP       = "f90356f9027fa0a478d87126fda71216fdf44eb837f062c1370070f2a7b554db6d67aeeafdfa24a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a02f3c0f783a82fb5193f94fe8dee03264c8f26101249a0db0a1a9afd218ae632da0e1e67e2a1f46fa10f7fe9c8fe95fd5fe9ce3271eee6c4a3438b129e4564854d8a006a7b8db3f8056e9d73d964f084ed588154ab64695c831d5fe946921052f2e2ab901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080018401c9c38082a0920a80a0000000000000000000000000000000000000000000000000000000000000000088000000000000000084342770c0a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a0bc90952194daa2ff522a0f8fcad9a96eefc86cd57846ffe276d08500ac71ce15a0e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855a0435218dcfcf660224d3af9fb98901165e5b4b37c2e60b10f4f4009188f86ef7701f8d0f8658084342770c0825208940000000000000000000000000000000000000a110180820a96a0f9e29c06b230c4038b5c5789d00ab0dc72bacdb5b9a40953822eadb9c4976c88a06ac40cf050140abc530e30555348c8760fc84efe9043e6d180a5252e57dc4accf8670184342770c08401030d40940000000000000000000000000000000000000b228080820a96a02235e5a510114d61239a0b6f1c5f4e7592f2ee101aa4146e9a3e0b25e554c2d0a07f306a5b187ff46d858a3cf6ff15055bfa0138c5a52164e03906a0a1754133b3c0c0"
	resumedBlockParentRLP = "f9025ba00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a042b6801d525e5c245d7d68d3eb584a17c6d20d64c8627cdc9d2da33f0c530511a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001808401c9c380808080a00000000000000000000000000000000000000000000000000000000000000000880000000000000000843b9aca00a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a00000000000000000000000000000000000000000000000000000000000000000a0e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

var (
	resumedBlockTarget   = common.HexToAddress("0x0000000000000000000000000000000000000a11")
	resumedBlockObserver = common.HexToAddress("0x0000000000000000000000000000000000000b22")
)

func resumedBlockFixture(t *testing.T) *blockreplay.Fixture {
	t.Helper()
	decode := func(s string) []byte {
		b, err := hex.DecodeString(s)
		require.NoError(t, err)
		return b
	}
	return &blockreplay.Fixture{
		BlockRLP:        decode(resumedBlockRLP),
		ParentHeaderRLP: decode(resumedBlockParentRLP),
		Senders:         [][20]byte{senderIsCoinbaseKey.rawAddress, senderIsCoinbaseKey.rawAddress},
	}
}

// withHeader returns block with the header changed by edit and the block access
// list bal attached.
func withHeader(block *types.Block, bal types.BlockAccessList, edit func(*types.Header)) *types.Block {
	header := types.CopyHeader(block.HeaderNoCopy())
	edit(header)
	return types.NewBlockFromNetwork(header, &types.Body{
		Transactions: block.Transactions(),
		Uncles:       block.Uncles(),
		Withdrawals:  block.Withdrawals(),
	}, types.NewBlockAccessListSidecar(bal))
}

// executeFixtureBlock executes block on top of its parent. When resumed, the
// state files end after the block's first tx, so only the second tx runs.
func executeFixtureBlock(t *testing.T, fixture *blockreplay.Fixture, parent *types.Header, config *chain.Config, block *types.Block, blockBAL types.BlockAccessList, resumed bool) *blockVerdict {
	t.Helper()
	observerCode := append([]byte{byte(vm.PUSH20)}, resumedBlockTarget[:]...)
	observerCode = append(observerCode, byte(vm.BALANCE), byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP))
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := newResumeTestDB(t)
	baselineTxNum := uint64(1)
	baselineBlockNum := uint64(0)
	if resumed {
		baselineTxNum = 101
		baselineBlockNum = 1
	}
	var baselineRoot []byte
	err := db.UpdateTemporal(ctx, func(seedTx kv.TemporalRwTx) error {
		seedDomains, seedErr := execctx.NewSharedDomains(ctx, seedTx, logger, execctx.WithParaTrieDB(db))
		if seedErr != nil {
			return seedErr
		}
		defer seedDomains.Close()
		seedDomains.SetTxNum(baselineTxNum)
		if resumed {
			if appendErr := rawdbv3.TxNums.Append(seedTx, 0, 99); appendErr != nil {
				return appendErr
			}
			if appendErr := rawdbv3.TxNums.Append(seedTx, 1, 103); appendErr != nil {
				return appendErr
			}
		}

		putAccount := func(address common.Address, account accounts.Account, code []byte) error {
			if putErr := seedDomains.DomainPut(kv.AccountsDomain, seedTx, address[:], accounts.SerialiseV3(&account), baselineTxNum, nil); putErr != nil {
				return putErr
			}
			if len(code) > 0 {
				return seedDomains.DomainPut(kv.CodeDomain, seedTx, address[:], code, baselineTxNum, nil)
			}
			return nil
		}

		senderAccount := accounts.NewAccount()
		senderAccount.Balance = *uint256.NewInt(1_000_000_000_000_000_000)
		if resumed {
			senderAccount.Nonce = 1
			senderAccount.Balance = *uint256.NewInt(999_981_625_000_000_000)
		}
		if err := putAccount(senderIsCoinbaseKey.rawAddress, senderAccount, nil); err != nil {
			return err
		}

		putContract := func(address common.Address, code []byte) error {
			account := accounts.NewAccount()
			account.Nonce = 1
			account.Incarnation = 1
			account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
			return putAccount(address, account, code)
		}
		if err := putContract(resumedBlockObserver, observerCode); err != nil {
			return err
		}
		for _, address := range []common.Address{
			config.GetWithdrawalRequestContract().Value(),
			config.GetConsolidationRequestContract().Value(),
			config.GetBuilderDepositContract().Value(),
			config.GetBuilderExitContract().Value(),
		} {
			if err := putContract(address, []byte{byte(vm.STOP)}); err != nil {
				return err
			}
		}
		baselineRoot, seedErr = seedDomains.ComputeCommitment(ctx, seedTx, true, baselineBlockNum, baselineTxNum, "resumed-block-seed", nil)
		if seedErr != nil {
			return seedErr
		}
		return seedDomains.Flush(ctx, seedTx)
	})
	require.NoError(t, err)
	if !resumed {
		require.Equal(t, parent.Root[:], baselineRoot)
	}

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback)
	domains, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	t.Cleanup(domains.Close)
	domains.SetTxNum(100)

	blockRLP, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	runFixture := &blockreplay.Fixture{
		BlockRLP: blockRLP, ParentHeaderRLP: fixture.ParentHeaderRLP,
		Senders: fixture.Senders,
	}
	blockReader, err := blockreplay.NewMemBlockReader(runFixture)
	require.NoError(t, err)
	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ExecWorkerCount = 2
	engine := rulesEngineWithErrors{Engine: merge.New(ethash.NewFaker())}
	cfg := StageExecuteBlocksCfg(db, prune.DefaultMode, 512*datasize.MB, config, engine, &vm.Config{}, nil, false, true, dirs, blockReader, nil, syncCfg, false, nil)
	source := &singleBlockSource{block: block, num: 1, parent: parent, bal: blockBAL}
	rng := execRange{blockNum: 1, initialTxNum: 99, inputTxNum: 100, maxBlockNum: 1}
	if resumed {
		rng.initialTxNum = 101
		rng.offsetFromBlockBeginning = 1
	}
	out, err := ExecV3(ctx, cfg, domains, rwTx, stages.ModeForkValidation, false, "resumed-block", rng, source, logger)
	require.NoError(t, err)
	return out.verdict
}

// A block resumed mid-block, because the state files end between two of its
// txs, executes only its remaining txs. It must be checked against every header
// commitment, as a fully executed block is.
func TestResumedBlockIsValidated(t *testing.T) {
	fixture := resumedBlockFixture(t)
	block, err := fixture.Block()
	require.NoError(t, err)
	parent, err := fixture.ParentHeader()
	require.NoError(t, err)
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = common.NewUint64(10)

	validBAL := types.BlockAccessList{
		{Address: common.Address{}},
		{Address: resumedBlockTarget},
		{Address: resumedBlockObserver, StorageReads: []common.Hash{{}}},
		{Address: params.HistoryStorageAddress.Value()},
		{Address: params.BeaconRootsAddress.Value()},
		{
			Address: senderIsCoinbaseKey.rawAddress,
			BalanceChanges: []*types.BalanceChange{
				{Index: 1, Value: *uint256.NewInt(999_981_625_000_000_000)},
				{Index: 2, Value: *uint256.NewInt(999_964_032_250_000_000)},
			},
			NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}, {Index: 2, Value: 2}},
		},
	}
	require.NoError(t, types.NewBlockAccessListSidecar(validBAL).ValidateForBlock(block.GasLimit(), len(block.Transactions())))
	validBALHash := validBAL.Hash()
	block = withHeader(block, validBAL, func(h *types.Header) { h.BlockAccessListHash = &validBALHash })

	phantomBAL := append(validBAL.Copy(), types.AccountChanges{Address: common.HexToAddress("0x0000000000000000000000000000000000000c33")})
	slices.SortFunc(phantomBAL, func(a, b types.AccountChanges) int { return bytes.Compare(a.Address[:], b.Address[:]) })
	phantomBALHash := phantomBAL.Hash()

	t.Run("valid", func(t *testing.T) {
		for _, resumed := range []bool{false, true} {
			if verdict := executeFixtureBlock(t, fixture, parent, config, block, validBAL, resumed); verdict != nil {
				t.Fatalf("resumed=%v: %v", resumed, verdict.err)
			}
		}
	})
	for _, tc := range []struct {
		name    string
		block   *types.Block
		bal     types.BlockAccessList
		fullErr string
	}{
		{"block access list", withHeader(block, phantomBAL, func(h *types.Header) { h.BlockAccessListHash = &phantomBALHash }), phantomBAL, "block access list mismatch"},
		{"receipts root", withHeader(block, validBAL, func(h *types.Header) { h.ReceiptHash = common.Hash{1} }), validBAL, "receipt"},
		{"state root", withHeader(block, validBAL, func(h *types.Header) { h.Root = common.Hash{2} }), validBAL, "root"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full := executeFixtureBlock(t, fixture, parent, config, tc.block, tc.bal, false)
			require.NotNil(t, full)
			require.Contains(t, strings.ToLower(full.err.Error()), tc.fullErr)
			require.NotNil(t, executeFixtureBlock(t, fixture, parent, config, tc.block, tc.bal, true), "the resumed block is accepted")
		})
	}
}
