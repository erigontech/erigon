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
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestForkChoiceLeavesSideBlocksOutOfDB(t *testing.T) {
	for _, mode := range []struct {
		name  string
		merge bool
	}{{"merged", true}, {"executed", false}} {
		t.Run(mode.name, func(t *testing.T) {
			m, key, _ := newMetricsTester(t)
			side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
			winner := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
			insertAndValidateBlocks(t, m, side, winner)
			if !mode.merge {
				m.ForkValidator.ClearWithUnwind()
			}
			updateForkChoiceTo(t, m, winner)

			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				require.NotNil(t, rawdb.ReadHeaderNumber(tx, winner.Hash()))
				require.Nil(t, rawdb.ReadHeaderNumber(tx, side.Hash()))
				return nil
			}))
		})
	}
}

func TestForkChoiceReorgsToRetainedSideBlock(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	winner := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
	insertAndValidateBlocks(t, m, side, winner)
	updateForkChoiceTo(t, m, winner)

	hash, number := side.Hash(), side.NumberU64()
	header, err := m.ExecModule.GetHeader(t.Context(), &hash, &number)
	require.NoError(t, err)
	require.NotNil(t, header)
	has, err := m.ExecModule.HasBlock(t.Context(), &hash, nil)
	require.NoError(t, err)
	require.True(t, has)

	updateForkChoiceTo(t, m, side)
	requireCanonicalWithTransactions(t, m, side)
}

func TestForkChoiceCommitFailureKeepsRetainedSideBlock(t *testing.T) {
	var failingDB *failOnceCommitDB
	m, key, _ := newMetricsTester(t, execmoduletester.WithRwDBWrapper(func(db kv.RwDB) kv.RwDB {
		failingDB = &failOnceCommitDB{RwDB: db}
		return failingDB
	}))
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	winner := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
	insertAndValidateBlocks(t, m, side, winner)
	updateForkChoiceTo(t, m, winner)

	failingDB.fail.Store(true)

	result, err := m.ExecModule.UpdateForkChoice(t.Context(), side.Hash(), common.Hash{}, common.Hash{})
	require.Error(t, err)
	require.Equal(t, execmodule.ForkChoiceResult{}, result)

	hash := side.Hash()
	has, err := m.ExecModule.HasBlock(t.Context(), &hash, nil)
	require.NoError(t, err)
	require.True(t, has)

	updateForkChoiceTo(t, m, side)
	requireCanonicalWithTransactions(t, m, side)
}

func TestForkChoiceGettersSeePublishedHead(t *testing.T) {
	var m *execmoduletester.ExecModuleTester
	var key *ecdsa.PrivateKey
	var child *types.Block
	var armed atomic.Bool
	type observation struct {
		header       *types.Header
		headerErr    error
		canonical    bool
		canonicalErr error
	}
	observed := make(chan observation, 1)
	m, key, _ = newMetricsTester(t, execmoduletester.WithStateTransitionObserver(func(ctx context.Context, point execmodule.StateTransitionPoint) {
		if point != execmodule.StateTransitionOverlayPublished || !armed.CompareAndSwap(true, false) {
			return
		}
		header, headerErr := m.ExecModule.CurrentHeader(ctx)
		canonical, canonicalErr := m.ExecModule.IsCanonicalHash(ctx, child.Hash())
		observed <- observation{header: header, headerErr: headerErr, canonical: canonical, canonicalErr: canonicalErr}
	}))
	m.ExecModule.SetPublishedSD(m.Notifications.Events.LatestSD)
	chain, err := m.GenerateChain(2, sendTo(t, m, key, common.Address{0x0c}, 1))
	require.NoError(t, err)
	base, child := chain.Blocks[0], chain.Blocks[1]
	insertAndValidateBlocks(t, m, base)
	updateForkChoiceTo(t, m, base)
	insertAndValidateBlocks(t, m, child)
	armed.Store(true)

	result, err := m.UpdateForkChoice(t.Context(), child.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	resultObservation := <-observed
	require.NoError(t, resultObservation.headerErr)
	require.Equal(t, child.Hash(), resultObservation.header.Hash())
	require.NoError(t, resultObservation.canonicalErr)
	require.True(t, resultObservation.canonical)
}

func TestValidateChildOfRetainedSideBlock(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	sideChain := generateTransferBlocks(t, m, key, 2, 0x0b)
	winner := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
	insertAndValidateBlocks(t, m, sideChain[0], winner)
	updateForkChoiceTo(t, m, winner)

	insertAndValidateBlocks(t, m, sideChain[1])
	updateForkChoiceTo(t, m, sideChain[1])
	requireCanonicalWithTransactions(t, m, sideChain[0])
	requireCanonicalWithTransactions(t, m, sideChain[1])
}

func TestRetainedSideBlockTransactionsSurviveLaterCommits(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	canonical := generateTransferBlocks(t, m, key, 3, 0x0a)
	insertAndValidateBlocks(t, m, side, canonical[0])
	updateForkChoiceTo(t, m, canonical[0])
	for _, block := range canonical[1:] {
		insertAndValidateBlocks(t, m, block)
		updateForkChoiceTo(t, m, block)
	}

	updateForkChoiceTo(t, m, side)
	requireCanonicalWithTransactions(t, m, side)
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		for _, block := range canonical {
			requireStoredTransactions(t, tx, block)
		}
		return nil
	}))
}

func TestInvalidPayloadKeepsValidSiblings(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	good := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
	bad := tamperBlockGasUsed(t, good, 21_000)
	insertAndValidateBlocks(t, m, side)

	_, err := m.InsertBlocks(t.Context(), []*types.Block{bad})
	require.NoError(t, err)
	result, err := m.ValidateChain(t.Context(), bad)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)

	hash := side.Hash()
	has, err := m.ExecModule.HasBlock(t.Context(), &hash, nil)
	require.NoError(t, err)
	require.True(t, has, "a valid sibling must survive the rejection of another payload")

	updateForkChoiceTo(t, m, side)
	requireCanonicalWithTransactions(t, m, side)

	badHash := bad.Hash()
	hasBad, err := m.ExecModule.HasBlock(t.Context(), &badHash, nil)
	require.NoError(t, err)
	require.False(t, hasBad, "a rejected payload must not be retained")
}

func TestInvalidRetainedSideBlockIsDropped(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	good := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	bad := tamperBlockGasUsed(t, good, 21_000)
	side := generateTransferBlocks(t, m, key, 1, 0x0c)[0]
	winner := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
	_, err := m.InsertBlocks(t.Context(), []*types.Block{bad})
	require.NoError(t, err)
	insertAndValidateBlocks(t, m, side, winner)
	updateForkChoiceTo(t, m, winner)

	badHash, badNumber := bad.Hash(), bad.NumberU64()
	has, err := m.ExecModule.HasBlock(t.Context(), &badHash, nil)
	require.NoError(t, err)
	require.True(t, has)

	result, err := m.ValidateChain(t.Context(), bad)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)

	header, err := m.ExecModule.GetHeader(t.Context(), &badHash, &badNumber)
	require.NoError(t, err)
	require.Nil(t, header)
	has, err = m.ExecModule.HasBlock(t.Context(), &badHash, nil)
	require.NoError(t, err)
	require.False(t, has)
	sideHash := side.Hash()
	has, err = m.ExecModule.HasBlock(t.Context(), &sideHash, nil)
	require.NoError(t, err)
	require.True(t, has)
	updateForkChoiceTo(t, m, side)
	requireCanonicalWithTransactions(t, m, side)
}

func TestRetainedBlockAboveCommittedSequenceKeepsTransactions(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	chainA := generateTransferBlocks(t, m, key, 2, 0x0a)
	chainB := generateTransferBlocks(t, m, key, 2, 0x0b)
	insertAndValidateBlocks(t, m, chainA[0], chainB[0])
	updateForkChoiceTo(t, m, chainA[0])
	updateForkChoiceTo(t, m, chainB[0])
	insertAndValidateBlocks(t, m, chainB[1])
	updateForkChoiceTo(t, m, chainA[0])
	insertAndValidateBlocks(t, m, chainA[1])
	updateForkChoiceTo(t, m, chainB[1])

	requireCanonicalWithTransactions(t, m, chainB[0])
	requireCanonicalWithTransactions(t, m, chainB[1])
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		requireStoredTransactions(t, tx, chainA[0])
		return nil
	}))
}

func TestRetainedSideBlockVisibleThroughInsertsAndForkChoices(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	m.ExecModule.SetPublishedSD(m.Notifications.Events.LatestSD)
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	canonical := generateTransferBlocks(t, m, key, 32, 0x0a)
	insertAndValidateBlocks(t, m, side, canonical[0])
	updateForkChoiceTo(t, m, canonical[0])

	hash := side.Hash()
	var misses, failures atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	var stopOnce sync.Once
	stopReader := func() {
		stopOnce.Do(func() { close(stop) })
		<-done
	}
	t.Cleanup(stopReader)
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			has, err := m.ExecModule.HasBlock(context.Background(), &hash, nil)
			switch {
			case err != nil:
				failures.Add(1)
			case !has:
				misses.Add(1)
			}
		}
	}()
	for _, block := range canonical[1:] {
		insertAndValidateBlocks(t, m, block)
		updateForkChoiceTo(t, m, block)
	}
	stopReader()
	require.Zero(t, failures.Load())
	require.Zero(t, misses.Load())
}

type forkChoiceLogProbe struct {
	next     log.Handler
	onSample func(string)
}

func (p *forkChoiceLogProbe) Enabled(context.Context, log.Lvl) bool { return true }

func (p *forkChoiceLogProbe) Log(record *log.Record) error {
	switch record.Msg {
	case "[updateForkChoice] dispatching notifications", "[dispatchNotifications] dispatching":
		p.onSample(record.Msg)
	}
	return p.next.Log(record)
}

func TestAdoptedSideBlockRemainsVisibleDuringForkChoice(t *testing.T) {
	for _, withOverlay := range []bool{false, true} {
		name := "without overlay"
		if withOverlay {
			name = "with overlay"
		}
		t.Run(name, func(t *testing.T) {
			m, key, _ := newMetricsTester(t)
			m.ExecModule.SetPublishedSD(m.Notifications.Events.LatestSD)
			side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
			canonical := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
			insertAndValidateBlocks(t, m, side, canonical)
			updateForkChoiceTo(t, m, canonical)

			if withOverlay {
				pendingPack, err := m.GenerateChainFrom(canonical, 1, func(_ int, block *blockgen.BlockGen) {
					block.SetExtra([]byte{0x0c})
				})
				require.NoError(t, err)
				pending := pendingPack.Blocks[0]
				_, err = m.InsertBlocks(t.Context(), []*types.Block{pending})
				require.NoError(t, err)
			}

			hash := side.Hash()
			observed := make(chan struct {
				message string
				has     bool
				err     error
			}, 2)
			previous := m.Log.GetHandler()
			m.Log.SetHandler(&forkChoiceLogProbe{
				next: previous,
				onSample: func(message string) {
					has, err := m.ExecModule.HasBlock(context.Background(), &hash, nil)
					observed <- struct {
						message string
						has     bool
						err     error
					}{message: message, has: has, err: err}
				},
			})
			t.Cleanup(func() { m.Log.SetHandler(previous) })

			updateForkChoiceTo(t, m, side)
			samples := make(map[string]struct {
				has bool
				err error
			}, 2)
			for range 2 {
				select {
				case result := <-observed:
					samples[result.message] = struct {
						has bool
						err error
					}{has: result.has, err: result.err}
				case <-time.After(5 * time.Second):
					t.Fatal("fork choice dispatch logs were not observed")
				}
			}
			for _, message := range []string{
				"[updateForkChoice] dispatching notifications",
				"[dispatchNotifications] dispatching",
			} {
				result, ok := samples[message]
				require.True(t, ok, message)
				require.NoError(t, result.err)
				require.True(t, result.has, message)
			}
		})
	}
}

func TestDeepSideChainSurvivesForkChoice(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	side := generateTransferBlocks(t, m, key, 20, 0x0b)
	canonical := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
	insertAndValidateBlocks(t, m, canonical)
	insertAndValidateBlocks(t, m, side...)
	updateForkChoiceTo(t, m, canonical)

	updateForkChoiceTo(t, m, side[len(side)-1])
	requireCanonicalWithTransactions(t, m, side[0])
	requireCanonicalWithTransactions(t, m, side[len(side)-1])
}

func TestForkChoiceToSideForkBelowFinalityIsInvalid(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	alt := generateTransferBlocks(t, m, key, 3, 0x0b)
	canon := generateTransferBlocks(t, m, key, 3, 0x0a)
	_, err := m.InsertBlocks(t.Context(), alt)
	require.NoError(t, err)
	_, err = m.InsertBlocks(t.Context(), canon)
	require.NoError(t, err)
	finality := []execmoduletester.UFCOpt{
		execmoduletester.WithSafeHash(canon[1].Hash()),
		execmoduletester.WithFinalisedHash(canon[0].Hash()),
	}
	result, err := m.UpdateForkChoice(t.Context(), canon[2].Header(), finality...)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())

	result, err = m.UpdateForkChoice(t.Context(), alt[2].Header(), finality...)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, result.Status)
}

func TestForkChoicePersistsSideBlockBeyondReorgDepth(t *testing.T) {
	const maxReorgDepth = 2
	m, key, _ := newMetricsTester(t, execmoduletester.WithMaxReorgDepth(maxReorgDepth))
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	canonical := generateTransferBlocks(t, m, key, maxReorgDepth+2, 0x0a)
	insertAndValidateBlocks(t, m, side, canonical[0])
	updateForkChoiceTo(t, m, canonical[0])
	for _, block := range canonical[1:] {
		insertAndValidateBlocks(t, m, block)
		updateForkChoiceTo(t, m, block)
	}

	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.NotNil(t, rawdb.ReadHeaderNumber(tx, side.Hash()))
		requireStoredTransactions(t, tx, side)
		return nil
	}))
	require.False(t, m.ExecModule.IsRetainedSideBlock(side.Hash()))

	result, err := m.UpdateForkChoice(t.Context(), side.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusReorgTooDeep, result.Status)
	m.ExecModule.WaitIdle(t.Context())
}

func TestValidateChainPurgesPersistedSideAncestor(t *testing.T) {
	for _, test := range []struct {
		name     string
		reinsert bool
	}{
		{name: "persisted-only"},
		{name: "pending-and-persisted", reinsert: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const maxReorgDepth = 4
			m, _, _ := newMetricsTester(t,
				execmoduletester.WithMaxReorgDepth(maxReorgDepth),
				execmoduletester.WithAlwaysGenerateChangesets(true),
			)
			prefixPack, err := m.GenerateChain(6, func(i int, block *blockgen.BlockGen) {
				block.SetExtra([]byte{0x01, byte(i)})
			})
			require.NoError(t, err)
			for _, block := range prefixPack.Blocks {
				insertAndValidateBlocks(t, m, block)
				updateForkChoiceTo(t, m, block)
			}

			sidePack, err := m.GenerateChainFrom(prefixPack.Blocks[3], 2, func(i int, block *blockgen.BlockGen) {
				block.SetExtra([]byte{0x0b, byte(i)})
			})
			require.NoError(t, err)
			persisted := tamperBlockGasUsed(t, sidePack.Blocks[0], 21_000)
			childHeader := types.CopyHeader(sidePack.Blocks[1].HeaderNoCopy())
			childHeader.ParentHash = persisted.Hash()
			badChild := types.NewBlockFromNetwork(childHeader, sidePack.Blocks[1].Body(), sidePack.Blocks[1].BlockAccessListSidecar())
			alternatePack, err := m.GenerateChainFrom(prefixPack.Blocks[5], 1, func(_ int, block *blockgen.BlockGen) {
				block.SetExtra([]byte{0x0c})
			})
			require.NoError(t, err)
			canonicalPack, err := m.GenerateChainFrom(prefixPack.Blocks[5], maxReorgDepth, func(i int, block *blockgen.BlockGen) {
				block.SetExtra([]byte{0x0a, byte(i)})
			})
			require.NoError(t, err)

			blocks := []*types.Block{persisted, badChild, alternatePack.Blocks[0]}
			blocks = append(blocks, canonicalPack.Blocks...)
			_, err = m.InsertBlocks(t.Context(), blocks)
			require.NoError(t, err)
			canonicalHead := canonicalPack.Blocks[len(canonicalPack.Blocks)-1]
			updateForkChoiceTo(t, m, canonicalHead)

			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				require.NotNil(t, rawdb.ReadHeaderNumber(tx, persisted.Hash()))
				require.Nil(t, rawdb.ReadHeaderNumber(tx, badChild.Hash()))
				return nil
			}))
			updateForkChoiceTo(t, m, alternatePack.Blocks[0])
			if test.reinsert {
				_, err = m.InsertBlocks(t.Context(), []*types.Block{persisted})
				require.NoError(t, err)
			}
			require.Equal(t, test.reinsert, m.ExecModule.IsRetainedSideBlock(persisted.Hash()))

			result, err := m.ValidateChain(t.Context(), badChild)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)
			for _, block := range []*types.Block{persisted, badChild} {
				require.False(t, m.ExecModule.IsRetainedSideBlock(block.Hash()))
			}
			updateForkChoiceTo(t, m, canonicalHead)

			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				for _, block := range []*types.Block{persisted, badChild} {
					require.Nil(t, rawdb.ReadHeaderNumber(tx, block.Hash()))
				}
				return nil
			}))
			bodies, err := m.ExecModule.GetPayloadBodiesByHash(t.Context(), []common.Hash{persisted.Hash(), badChild.Hash()})
			require.NoError(t, err)
			require.Len(t, bodies, 2)
			for _, body := range bodies {
				require.Nil(t, body)
			}
		})
	}
}

func TestForkChoiceBoundsRetainedSideBlocks(t *testing.T) {
	const maxReorgDepth = 4
	m, _, _ := newMetricsTester(t, execmoduletester.WithMaxReorgDepth(maxReorgDepth))
	basePack, err := m.GenerateChain(1, func(_ int, block *blockgen.BlockGen) {
		block.SetExtra([]byte{0x01})
	})
	require.NoError(t, err)
	base := basePack.Blocks[0]
	insertAndValidateBlocks(t, m, base)
	updateForkChoiceTo(t, m, base)

	unused := make([]*types.Block, 0, maxReorgDepth+4)
	for i := range maxReorgDepth + 4 {
		canonicalPack, err := m.GenerateChainFrom(base, 1, func(_ int, block *blockgen.BlockGen) {
			block.SetExtra([]byte{byte(i), 0x0a})
		})
		require.NoError(t, err)
		sidePack, err := m.GenerateChainFrom(base, 1, func(_ int, block *blockgen.BlockGen) {
			block.SetExtra([]byte{byte(i), 0x0b})
		})
		require.NoError(t, err)
		canonical, side := canonicalPack.Blocks[0], sidePack.Blocks[0]
		insertAndValidateBlocks(t, m, side, canonical)
		updateForkChoiceTo(t, m, canonical)
		unused = append(unused, side)
	}
	sort.Slice(unused, func(i, j int) bool {
		left, right := unused[i].Hash(), unused[j].Hash()
		return bytes.Compare(left[:], right[:]) < 0
	})

	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		for _, block := range unused[:maxReorgDepth] {
			require.NotNil(t, rawdb.ReadHeaderNumber(tx, block.Hash()))
			requireStoredTransactions(t, tx, block)
		}
		for _, block := range unused[maxReorgDepth:] {
			require.Nil(t, rawdb.ReadHeaderNumber(tx, block.Hash()))
		}
		return nil
	}))
	for _, block := range unused {
		hash := block.Hash()
		has, err := m.ExecModule.HasBlock(t.Context(), &hash, nil)
		require.NoError(t, err)
		require.True(t, has)
	}
}

func TestFinalizedForkChoiceDropsRetainedSideBlock(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	canonical := generateTransferBlocks(t, m, key, 2, 0x0a)
	insertAndValidateBlocks(t, m, side, canonical[0])
	updateForkChoiceTo(t, m, canonical[0])
	hash := side.Hash()
	has, err := m.ExecModule.HasBlock(t.Context(), &hash, nil)
	require.NoError(t, err)
	require.True(t, has)

	insertAndValidateBlocks(t, m, canonical[1])
	result, err := m.UpdateForkChoice(t.Context(), canonical[1].Header(),
		execmoduletester.WithSafeHash(canonical[0].Hash()),
		execmoduletester.WithFinalisedHash(canonical[0].Hash()))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())

	has, err = m.ExecModule.HasBlock(t.Context(), &hash, nil)
	require.NoError(t, err)
	require.False(t, has)
}

func generateTransferBlocks(t *testing.T, m *execmoduletester.ExecModuleTester, key *ecdsa.PrivateKey, n int, to byte) []*types.Block {
	t.Helper()
	chain, err := m.GenerateChain(n, sendTo(t, m, key, common.Address{to}, 1))
	require.NoError(t, err)
	return chain.Blocks
}

func insertAndValidateBlocks(t *testing.T, m *execmoduletester.ExecModuleTester, blocks ...*types.Block) {
	t.Helper()
	for _, block := range blocks {
		_, err := m.InsertBlocks(t.Context(), []*types.Block{block})
		require.NoError(t, err)
		result, err := m.ValidateChain(t.Context(), block)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	}
}

func updateForkChoiceTo(t *testing.T, m *execmoduletester.ExecModuleTester, head *types.Block) {
	t.Helper()
	result, err := m.UpdateForkChoice(t.Context(), head.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
}

func requireCanonicalWithTransactions(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block) {
	t.Helper()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		hash, ok, err := m.BlockReader.CanonicalHash(t.Context(), tx, block.NumberU64())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, block.Hash(), hash)
		requireStoredTransactions(t, tx, block)
		return nil
	}))
}

func requireStoredTransactions(t *testing.T, tx kv.Tx, block *types.Block) {
	t.Helper()
	body, err := rawdb.ReadBodyWithTransactions(tx, block.Hash(), block.NumberU64())
	require.NoError(t, err)
	require.NotNil(t, body)
	require.Len(t, body.Transactions, len(block.Transactions()))
	for i, txn := range body.Transactions {
		require.Equal(t, block.Transactions()[i].Hash(), txn.Hash())
	}
}

type failOnceCommitDB struct {
	kv.RwDB
	fail atomic.Bool
}

func (db *failOnceCommitDB) BeginRw(ctx context.Context) (kv.RwTx, error) {
	tx, err := db.RwDB.BeginRw(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	rollback := true
	defer func() {
		if rollback {
			tx.Rollback()
		}
	}()
	if db.fail.CompareAndSwap(true, false) {
		rollback = false
		return &failOnceCommitTx{RwTx: tx}, nil
	}
	rollback = false
	return tx, nil
}

type failOnceCommitTx struct {
	kv.RwTx
}

func (tx *failOnceCommitTx) Commit() error {
	tx.RwTx.Rollback()
	return errors.New("forced commit failure")
}
