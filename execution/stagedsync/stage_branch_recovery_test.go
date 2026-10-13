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

package stagedsync_test

import (
	"encoding/hex"
	"math/big"
	"slices"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// Rejection and ordinary reorgs must preserve valid branches and converge to
// clean execution. Use DefaultStages: the FCU pipeline omits header unwinding.
func TestStagedSyncBranchRecovery(t *testing.T) {
	ctx := t.Context()
	genesis, key, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	newTester := func(t *testing.T) *execmoduletester.ExecModuleTester {
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key), execmoduletester.WithStepSize(32))
		m.ExecModule.WaitIdle(t.Context())
		return m
	}
	reference := newTester(t)
	const prefix, length, continuation = 4, 40, 3
	canonical := buildRecoveryChain(t, reference, length+continuation, prefix, 0)
	alternative := buildRecoveryChain(t, reference, length, prefix, 1_000)
	require.Equal(t, canonical.Blocks[prefix-1].Hash(), alternative.Blocks[prefix-1].Hash())
	require.NotEqual(t, canonical.Blocks[prefix].Hash(), alternative.Blocks[prefix].Hash())
	require.NoError(t, reference.InsertValidateAndUfc1By1(ctx, canonical.Blocks[:length]))
	reference.ExecModule.WaitIdle(ctx)
	wantRecovered := readRecoveryState(t, reference)
	require.NoError(t, reference.InsertValidateAndUfc1By1(ctx, canonical.Blocks[length:]))
	reference.ExecModule.WaitIdle(ctx)
	wantContinued := readRecoveryState(t, reference)

	for _, tc := range []struct {
		name  string
		badAt int
	}{
		{"reorg", 0},
		{"first_unexecuted", prefix + 1},
		{"middle", prefix + (length-prefix)/2},
		{"tip", length},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newTester(t)
			branch := alternative.Blocks
			validBranchEnd := len(branch)
			var rejected []*types.Block
			if tc.badAt != 0 {
				branch = corruptRecoveryBranch(branch, tc.badAt)
				validBranchEnd = tc.badAt - 1
				rejected = branch[tc.badAt-1:]
				require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), branch[:prefix]))
			} else {
				require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), branch))
			}
			m.ExecModule.WaitIdle(t.Context())
			valid := slices.Concat(canonical.Blocks[:length], branch[:validBranchEnd])
			allBlocks := slices.Concat(canonical.Blocks[:length], branch)

			// Download both branches before rejection. Recovery may select known
			// blocks, but must not repair their indexes by importing them again.
			downloadRecoveryBlocks(t, m, allBlocks)
			if tc.badAt != 0 {
				require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
					selectRecoveryBranch(t, tx, branch[prefix:])
					return nil
				}))
			}
			tx, err := m.DB.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			assertRecoveryHeaders(t, m, tx, allBlocks, nil)
			domains, err := execctx.NewSharedDomains(t.Context(), tx, m.Log)
			require.NoError(t, err)
			defer func() { domains.Close() }()
			if tc.badAt != 0 {
				_, err = m.Sync.Run(domains, tx, false, false)
				require.ErrorIs(t, err, rules.ErrInvalidBlock)
				require.NotNil(t, m.Sync.PrevUnwindPoint(), "rejection must execute a staged unwind")
				require.Less(t, *m.Sync.PrevUnwindPoint(), uint64(tc.badAt))
				assertRecoveryUnwind(t, tx, *m.Sync.PrevUnwindPoint())
				assertRecoveryHeaders(t, m, tx, valid, rejected)
				// Failed execution is not flushed. Keep the stage unwind's metadata
				// in the caller-owned transaction while discarding its state batch.
				domains.Close()
				domains, err = execctx.NewSharedDomains(t.Context(), tx, m.Log)
				require.NoError(t, err)
			}
			require.NoError(t, m.Sync.UnwindTo(prefix, stagedsync.ForkChoice, tx))
			require.NoError(t, m.Sync.RunUnwind(domains, tx))
			assertRecoveryUnwind(t, tx, prefix)
			assertRecoveryHeaders(t, m, tx, valid, rejected)
			require.NoError(t, domains.Commit(t.Context(), tx))
			domains.Close()
			runRecoveryPipeline(t, m, canonical.Blocks[prefix:length])
			assertRecoveryState(t, m, valid, rejected, wantRecovered)

			// Reload committed state and run forward again before extending it.
			runRecoveryPipeline(t, m, nil)
			assertRecoveryState(t, m, valid, rejected, wantRecovered)
			downloadRecoveryBlocks(t, m, canonical.Blocks[length:])
			runRecoveryPipeline(t, m, canonical.Blocks[length:])
			assertRecoveryState(t, m, append(valid, canonical.Blocks[length:]...), rejected, wantContinued)
		})
	}
}

func buildRecoveryChain(t *testing.T, m *execmoduletester.ExecModuleTester, length, prefix int, seedOffset int64) *blockgen.ChainPack {
	t.Helper()
	contractABI, err := abi.JSON(strings.NewReader(contracts.StateChurnABI))
	require.NoError(t, err)
	var nonce uint64
	var contract common.Address
	chainPack, err := m.GenerateChain(length, func(i int, block *blockgen.BlockGen) {
		addTx := func(txn types.Transaction) {
			signed, signErr := types.SignTx(txn, *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
			require.NoError(t, signErr)
			block.AddTx(signed)
			nonce++
		}
		var txn types.Transaction
		const contractLifetime = 20
		if i%contractLifetime == 0 {
			contract = types.CreateAddress(m.Address, nonce)
			txn = types.NewContractCreation(nonce, new(uint256.Int), params.MaxTxnGasLimit, m.Genesis.BaseFee(), common.FromHex(contracts.StateChurnBin))
		} else {
			seed := int64(i)
			if i >= prefix {
				seed += seedOffset
			}
			data, packErr := contractABI.Pack("poke", big.NewInt(seed))
			require.NoError(t, packErr)
			txn = types.NewTransaction(nonce, contract, new(uint256.Int), params.MaxTxnGasLimit, m.Genesis.BaseFee(), data)
		}
		addTx(txn)
		// Different transaction counts also change TxNums and contract addresses.
		if seedOffset != 0 && i >= prefix && i%3 == 0 {
			addTx(types.NewTransaction(nonce, common.Address{0xaa}, uint256.NewInt(1), params.MaxTxnGasLimit, m.Genesis.BaseFee(), nil))
		}
	})
	require.NoError(t, err)
	for i, receipts := range chainPack.Receipts {
		for _, receipt := range receipts {
			require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status, "fixture block %d gas %d", i+1, receipt.GasUsed)
		}
	}
	return chainPack
}

func corruptRecoveryBranch(blocks []*types.Block, badAt int) []*types.Block {
	branch := append([]*types.Block(nil), blocks...)
	for i := badAt - 1; i < len(branch); i++ {
		header := branch[i].Header()
		header.ParentHash = branch[i-1].Hash()
		if i == badAt-1 {
			header.Root[0] ^= 1
		}
		branch[i] = types.NewBlockFromNetwork(header, branch[i].Body(), branch[i].BlockAccessListSidecar())
	}
	return branch
}

func assertRecoveryHeaders(t *testing.T, m *execmoduletester.ExecModuleTester, tx kv.Tx, valid, rejected []*types.Block) {
	t.Helper()
	for _, block := range valid {
		number := rawdb.ReadHeaderNumber(tx, block.Hash())
		require.NotNil(t, number, "valid block %d %s must remain readable", block.NumberU64(), block.Hash())
		require.Equal(t, block.NumberU64(), *number)
		badNumber, readErr := rawdb.ReadBadHeaderNumber(tx, block.Hash())
		require.NoError(t, readErr)
		require.Nil(t, badNumber, "valid block %d %s must not be marked bad", block.NumberU64(), block.Hash())
		header, readErr := m.BlockReader.HeaderByHash(t.Context(), tx, block.Hash())
		require.NoError(t, readErr)
		require.NotNil(t, header)
		require.Equal(t, block.Hash(), header.Hash())
	}
	for _, block := range rejected {
		require.Nil(t, rawdb.ReadHeaderNumber(tx, block.Hash()), "rejected branch block %d %s must stay unindexed", block.NumberU64(), block.Hash())
		header, readErr := m.BlockReader.HeaderByHash(t.Context(), tx, block.Hash())
		require.NoError(t, readErr)
		require.Nil(t, header)
	}
}

func assertRecoveryUnwind(t *testing.T, tx kv.Tx, point uint64) {
	t.Helper()
	next, err := rawdb.ReadCanonicalHash(tx, point+1)
	require.NoError(t, err)
	require.Zero(t, next, "canonical chain must stop at the unwind point")
	lastBlock, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.Equal(t, point, lastBlock, "transaction numbering must stop at the unwind point")
}

func assertRecoveryState(t *testing.T, m *execmoduletester.ExecModuleTester, valid, rejected []*types.Block, want recoveryState) {
	t.Helper()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		assertRecoveryHeaders(t, m, tx, valid, rejected)
		return nil
	}))
	got := readRecoveryState(t, m)
	require.Equal(t, want.Head, got.Head)
	require.Equal(t, want.Canonical, got.Canonical)
	require.Equal(t, want.TxNums, got.TxNums)
	require.Equal(t, want.TxLookup, got.TxLookup, "canonical transaction lookup")
	for domain, values := range want.Domains {
		require.Equal(t, values, got.Domains[domain], "%s latest state", domain)
	}
	for domain, history := range want.History {
		require.Equal(t, history, got.History[domain], "%s history", domain)
	}
}

type recoveryState struct {
	Head      common.Hash
	Canonical []common.Hash
	TxNums    []uint64
	Domains   map[kv.Domain]map[string]string
	TxLookup  map[string]string
	History   map[kv.Domain][]recoveryHistoryEntry
}

type recoveryHistoryEntry struct {
	Key, Before string
	TxNum       uint64
}

func readRecoveryState(t *testing.T, m *execmoduletester.ExecModuleTester) recoveryState {
	t.Helper()
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	head := rawdb.ReadHeadBlockHash(tx)
	require.Equal(t, head, rawdb.ReadHeadHeaderHash(tx))
	header, err := rawdb.ReadHeaderByHash(tx, head)
	require.NoError(t, err)
	require.NotNil(t, header)
	for _, id := range m.Sync.StagesIdsList() {
		if stages.SyncStage(id) == stages.Snapshots {
			continue
		}
		progress, readErr := stages.GetStageProgress(tx, stages.SyncStage(id))
		require.NoError(t, readErr)
		require.Equal(t, header.Number.Uint64(), progress, "%s must reach the recovered head", id)
	}
	encoded, _, err := tx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	root, blockNum, txNum, err := commitment.HexTrieExtractStateRoot(encoded)
	require.NoError(t, err)
	require.Equal(t, header.Number.Uint64(), blockNum)
	require.Equal(t, header.Root, common.BytesToHash(root))
	lastBlock, lastTxNum, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.Equal(t, blockNum, lastBlock)
	require.Equal(t, txNum, lastTxNum, "transaction numbering must end at the commitment boundary")
	require.GreaterOrEqual(t, txNum, m.DB.StepSize(), "fixture must cross a domain step")
	require.Empty(t, tx.Debug().DomainFiles(), "fixture must remain fully unwindable")
	result := recoveryState{
		Head: head, Canonical: make([]common.Hash, blockNum+1), TxNums: make([]uint64, blockNum+1),
		Domains: make(map[kv.Domain]map[string]string), TxLookup: make(map[string]string),
		History: make(map[kv.Domain][]recoveryHistoryEntry),
	}
	for block := range result.Canonical {
		result.Canonical[block], err = rawdb.ReadCanonicalHash(tx, uint64(block))
		require.NoError(t, err)
		result.TxNums[block], err = rawdbv3.TxNums.Max(t.Context(), tx, uint64(block))
		require.NoError(t, err)
	}
	next, err := rawdb.ReadCanonicalHash(tx, blockNum+1)
	require.NoError(t, err)
	require.Zero(t, next, "canonical chain must end at the recovered head")
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain, kv.ReceiptDomain} {
		result.Domains[domain] = readRecoveryDomain(t, tx, domain)
		require.NotEmpty(t, result.Domains[domain], "%s latest state must be exercised", domain)
	}
	require.NoError(t, tx.ForEach(kv.TxLookup, nil, func(key, value []byte) error {
		result.TxLookup[hex.EncodeToString(key)] = hex.EncodeToString(value)
		return nil
	}))
	require.NotEmpty(t, result.TxLookup)
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.ReceiptDomain} {
		result.History[domain] = readRecoveryHistory(t, tx, domain)
		require.NotEmpty(t, result.History[domain], "%s history must be exercised", domain)
	}
	return result
}

func readRecoveryDomain(t *testing.T, tx kv.TemporalTx, domain kv.Domain) map[string]string {
	t.Helper()
	entries, err := tx.Debug().RangeLatest(domain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer entries.Close()
	values := make(map[string]string)
	for entries.HasNext() {
		key, value, err := entries.Next()
		require.NoError(t, err)
		if len(value) != 0 {
			values[hex.EncodeToString(key)] = hex.EncodeToString(value)
		}
	}
	entries.Close()
	return values
}

func readRecoveryHistory(t *testing.T, tx kv.TemporalTx, domain kv.Domain) []recoveryHistoryEntry {
	t.Helper()
	// Scan without an upper bound to detect history left above the head.
	entries, err := tx.Debug().HistoryKeyTxNumRange(domain, -1, -1, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer entries.Close()
	var history []recoveryHistoryEntry
	for entries.HasNext() {
		key, txNum, err := entries.Next()
		require.NoError(t, err)
		before, ok, err := tx.HistorySeek(domain, key, txNum)
		require.NoError(t, err)
		require.True(t, ok, "%s history index must have a value", domain)
		history = append(history, recoveryHistoryEntry{
			Key: hex.EncodeToString(key), Before: hex.EncodeToString(before), TxNum: txNum,
		})
	}
	entries.Close()
	return history
}

func downloadRecoveryBlocks(t *testing.T, m *execmoduletester.ExecModuleTester, blocks []*types.Block) {
	t.Helper()
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		for _, block := range blocks {
			if rawdb.ReadHeader(tx, block.Hash(), block.NumberU64()) == nil {
				require.NoError(t, rawdb.WriteBlock(tx, block))
				parentTD, err := rawdb.ReadTd(tx, block.ParentHash(), block.NumberU64()-1)
				require.NoError(t, err)
				require.NotNil(t, parentTD)
				td := new(uint256.Int).Add(parentTD, &block.HeaderNoCopy().Difficulty)
				require.NoError(t, rawdb.WriteTd(tx, block.Hash(), block.NumberU64(), *td))
				if bal := block.BlockAccessListSidecar(); bal != nil {
					encoded, err := bal.Bytes()
					require.NoError(t, err)
					require.NoError(t, rawdb.WriteBlockAccessListBytes(tx, block.Hash(), block.NumberU64(), encoded))
				}
			}
		}
		return nil
	}))
}

// Selection updates download progress, but must not repair header indexes.
func selectRecoveryBranch(t *testing.T, tx kv.RwTx, blocks []*types.Block) {
	t.Helper()
	for _, block := range blocks {
		require.NoError(t, rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64()))
	}
	head := blocks[len(blocks)-1]
	require.NoError(t, rawdb.WriteHeadHeaderHash(tx, head.Hash()))
	for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies} {
		require.NoError(t, stages.SaveStageProgress(tx, stage, head.NumberU64()))
	}
	last, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.NoError(t, rawdb.AppendCanonicalTxNums(tx, last+1))
}

func runRecoveryPipeline(t *testing.T, m *execmoduletester.ExecModuleTester, blocks []*types.Block) {
	t.Helper()
	if len(blocks) > 0 {
		require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
			selectRecoveryBranch(t, tx, blocks)
			return nil
		}))
	}
	tx, err := m.DB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, m.Log)
	require.NoError(t, err)
	defer domains.Close()
	more, err := m.Sync.Run(domains, tx, false, false)
	require.NoError(t, err)
	require.False(t, more)
	require.NoError(t, domains.Commit(t.Context(), tx))
}
