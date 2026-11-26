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
	"context"
	"encoding/binary"
	"math"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	frTxs        = 18
	frBackground = 200_000
)

var frSlot = common.Hash{31: 1}

type frState struct {
	addrs     []common.Address
	accs      []accounts.Account
	contracts int
}

func newFRState(n, contracts int) *frState {
	st := &frState{addrs: make([]common.Address, n), accs: make([]accounts.Account, n), contracts: contracts}
	for i := range n {
		st.addrs[i] = frAddr(0, uint64(i))
		st.accs[i] = accounts.Account{Nonce: 1, Balance: *uint256.NewInt(uint64(i) + 1), CodeHash: accounts.EmptyCodeHash}
		if i < contracts {
			st.accs[i].CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(st.addrs[i][:]))
		}
	}
	return st
}

func frAddr(tag, i uint64) common.Address {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], tag)
	binary.BigEndian.PutUint64(buf[8:], i)
	return common.BytesToAddress(crypto.Keccak256(buf[:]))
}

func frSortBAL(bl types.BlockAccessList) types.BlockAccessList {
	slices.SortFunc(bl, func(a, b types.AccountChanges) int { return a.Address.Cmp(b.Address) })
	return bl
}

// cmaBlock: 18 senders and a coinbase change; each tx also CALLs ~6,000 cold absent addresses, which the BAL lists with no change.
func cmaBlock(n uint64, st *frState) (types.BlockAccessList, []int) {
	bl := make(types.BlockAccessList, 0, frTxs*6001+1)
	coinbase := frTxs
	cb := types.AccountChanges{Address: st.addrs[coinbase]}
	changed := []int{coinbase}
	for tx := range frTxs {
		idx := uint32(tx + 1)
		st.accs[tx].Nonce++
		st.accs[tx].Balance.SubUint64(&st.accs[tx].Balance, 1)
		st.accs[coinbase].Balance.AddUint64(&st.accs[coinbase].Balance, 1)
		bl = append(bl, types.AccountChanges{
			Address:        st.addrs[tx],
			BalanceChanges: []*types.BalanceChange{{Index: idx, Value: st.accs[tx].Balance}},
			NonceChanges:   []*types.NonceChange{{Index: idx, Value: st.accs[tx].Nonce}},
		})
		cb.BalanceChanges = append(cb.BalanceChanges, &types.BalanceChange{Index: idx, Value: st.accs[coinbase].Balance})
		changed = append(changed, tx)
		for j := range 6000 {
			bl = append(bl, types.AccountChanges{Address: frAddr(n, uint64(tx*6000+j))})
		}
	}
	return frSortBAL(append(bl, cb)), changed
}

// sdeBlock: value-bearing SELFDESTRUCT of existing contracts; each contract and its beneficiary get a balance change.
func sdeBlock(n uint64, st *frState) (types.BlockAccessList, []int) {
	pairs := st.contracts
	bl := make(types.BlockAccessList, 0, 2*pairs)
	changed := make([]int, 0, 2*pairs)
	for j := range pairs {
		idx := uint32(j%frTxs + 1)
		c, ben := j, pairs+j
		v := st.accs[c].Balance
		st.accs[ben].Balance.Add(&st.accs[ben].Balance, &v)
		st.accs[c].Balance = *uint256.NewInt(n)
		bl = append(bl,
			types.AccountChanges{Address: st.addrs[c], BalanceChanges: []*types.BalanceChange{{Index: idx, Value: st.accs[c].Balance}}},
			types.AccountChanges{Address: st.addrs[ben], BalanceChanges: []*types.BalanceChange{{Index: idx, Value: st.accs[ben].Balance}}},
		)
		changed = append(changed, c, ben)
	}
	return frSortBAL(bl), changed
}

func frOpen(b *testing.B, dir string) (kv.TemporalRwDB, func()) {
	logger := log.New()
	dirs := datadir.New(dir)
	rawDb := mdbx.New(dbcfg.ChainDB, logger).Path(dirs.Chaindata).MustOpen()
	agg, err := dbstate.NewTest(dirs).StepSize(1 << 30).Logger(logger).Open(context.Background())
	require.NoError(b, err)
	db, err := temporal.New(rawDb, agg, nil)
	require.NoError(b, err)
	return db, func() { agg.Close(); rawDb.Close() }
}

// frCommitState writes st as of block blockNum into a fresh tx and returns the root a from-scratch fold gives.
func frCommitState(b *testing.B, db kv.TemporalRwDB, st *frState, blockNum, txNum uint64) []byte {
	ctx := context.Background()
	tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
	require.NoError(b, err)
	defer tx.Rollback()
	doms, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(b, err)
	defer doms.Close()
	for k := range blockNum {
		require.NoError(b, rawdbv3.TxNums.Append(tx, k, k))
	}
	require.NoError(b, rawdbv3.TxNums.Append(tx, blockNum, txNum))
	for i := range st.addrs {
		require.NoError(b, doms.DomainPut(kv.AccountsDomain, tx, st.addrs[i][:], accounts.SerialiseV3(&st.accs[i]), txNum, nil))
		if i < st.contracts {
			require.NoError(b, doms.DomainPut(kv.StorageDomain, tx, append(st.addrs[i][:], frSlot[:]...), []byte{1}, txNum, nil))
		}
	}
	root, err := doms.ComputeCommitment(ctx, tx, true, blockNum, txNum, "", nil)
	require.NoError(b, err)
	require.NoError(b, doms.Flush(ctx, tx))
	require.NoError(b, tx.Commit())
	return root
}

type frPhases struct {
	names []string
	wall  []time.Duration
	cpu   []time.Duration
	at    time.Time
	atCPU time.Duration
}

func frCPU() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func (p *frPhases) start() { p.at, p.atCPU = time.Now(), frCPU() }

func (p *frPhases) mark(i int, name string) {
	now, c := time.Now(), frCPU()
	if i == len(p.names) {
		p.names, p.wall, p.cpu = append(p.names, name), append(p.wall, 0), append(p.cpu, 0)
	}
	p.wall[i] += now.Sub(p.at)
	p.cpu[i] += c - p.atCPU
	p.at, p.atCPU = now, c
}

// runFinalRound replays computeBlockFromBAL's window-block path phase by phase over MDBX-backed state.
func runFinalRound(b *testing.B, st *frState, block func(uint64, *frState) (types.BlockAccessList, []int)) {
	setAheadFlags(b, true)
	ctx := context.Background()
	db, closeDB := frOpen(b, b.TempDir())
	defer closeDB()
	frCommitState(b, db, st, 1, 1)

	tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
	require.NoError(b, err)
	defer tx.Rollback()
	doms, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(b, err)
	defer doms.Close()
	doms.SetDeferCommitmentUpdates(true)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "bench", log.New(), false, 2, make(chan applyResult, 1), nil, make(chan commitmentResult, 1), state.NewLayeredDomainReader(doms, nil, nil), nil)
	require.NoError(b, err)
	defer cc.Stop()
	require.NotNil(b, cc.branchPrefetch, "commitment v3 feed path expected")

	var p frPhases
	var root []byte
	var lastTxNum uint64
	b.ResetTimer()
	for i := range b.N {
		n := uint64(i) + 2
		firstTxNum := 2 + uint64(i)*(frTxs+2)
		lastTxNum = firstTxNum + frTxs + 1
		hash := common.Hash{0: 0xfe, 24: byte(n >> 8), 31: byte(n)}
		b.StopTimer()
		bl, changed := block(n, st)
		cs := &changeset.StateChangeSet{}
		doms.SavePastChangesetAccumulator(hash, n, cs)
		doms.SetChangesetAccumulator(cs)
		b.StartTimer()

		p.start()
		cc.prefetchWrites(bal.ToWriteSet(bl, math.MaxUint32))
		p.mark(0, "request")
		vm := state.NewVersionMap(bl)
		vm.SetBaseTxNum(firstTxNum)
		base := newMemoAccountReader(cc.asOfReader.dr.StateReaderAt(firstTxNum))
		ambiguous, err := vm.HasAmbiguousBaseAccount(true, false, base)
		require.NoError(b, err)
		require.False(b, ambiguous)
		require.NoError(b, vm.SeedEmptyRemovals(true, false, base))
		p.mark(1, "prep")

		reader := &asOfStateReader{sd: cc.doms, roTx: cc.roTx, txNum: lastTxNum + 1, dr: cc.asOfReader.dr, prefetched: cc.branchPrefetch, cur: vm, curTxNum: lastTxNum}
		if cc.balUpdates == nil {
			cc.balUpdates = cc.updates.NewEmpty()
		} else {
			cc.balUpdates.Reset()
		}
		cc.clearTouched()
		cc.balBlock = true
		ws := bal.ToWriteSetAfter(bl, -1, math.MaxUint32)
		p.mark(2, "writeSet")
		cc.accumulateTouched(ws, nil)
		p.mark(3, "accumulate")
		require.NoError(b, cc.emit(cc.foldView(vm, lastTxNum, base), cc.balUpdates))
		cc.clearFoldedKeys()
		p.mark(4, "emit")
		cc.finishPrefetch()
		sdCtx := cc.doms.GetCommitmentContext()
		sdCtx.SetFeed(&cc.feed.Feed)
		sdCtx.SetStateReader(reader)
		require.NoError(b, cc.doms.FlushPendingUpdates(cc.roTx))
		p.mark(5, "flushPrev")
		diff := &cc.doms.GetChangesetByHash(n, hash).Diffs[kv.CommitmentDomain]
		root, err = sdCtx.ComputeCommitmentWithDiff(ctx, cc.roTx, true, n, lastTxNum, "bench", nil, diff)
		require.NoError(b, err)
		if upd := sdCtx.PeekPendingUpdate(); upd != nil && upd.BlockNum == n {
			upd.BlockHash = hash
		}
		p.mark(6, "fold")

		b.StopTimer()
		require.Equal(b, len(changed), cc.feed.Keys, "the fold must see only changed accounts")
		for _, j := range changed {
			require.NoError(b, doms.DomainPut(kv.AccountsDomain, tx, st.addrs[j][:], accounts.SerialiseV3(&st.accs[j]), lastTxNum, nil))
		}
		b.StartTimer()
	}
	b.StopTimer()
	for i, name := range p.names {
		b.ReportMetric(float64(p.wall[i].Microseconds())/1000/float64(b.N), name+"-ms")
		b.ReportMetric(float64(p.cpu[i].Microseconds())/1000/float64(b.N), name+"-cpums")
	}

	check, closeCheck := frOpen(b, b.TempDir())
	defer closeCheck()
	require.Equal(b, frCommitState(b, check, st, uint64(b.N)+1, lastTxNum), root, "the replayed final round must match a from-scratch fold")
}

// BenchmarkComputeAheadFinalRound bounds the BAL compute-ahead final round on a cma-like and an sde-like block.
// Run with a fixed -benchtime=Nx: every iteration is the next block.
func BenchmarkComputeAheadFinalRound(b *testing.B) {
	b.Run("cma", func(b *testing.B) { runFinalRound(b, newFRState(frBackground, 0), cmaBlock) })
	b.Run("sde", func(b *testing.B) { runFinalRound(b, newFRState(frBackground, 18_442), sdeBlock) })
}
