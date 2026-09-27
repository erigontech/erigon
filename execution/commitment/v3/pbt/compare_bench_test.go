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

package pbt_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"os/exec"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

type feedBenchLeaf struct {
	plain []byte
	key   []byte
	value [eip8297.ValueLength]byte
	state commitment.Update
}

type feedBenchFixture struct {
	seed  []feedBenchLeaf
	batch []feedBenchLeaf
}

type feedBenchContext struct {
	mu       sync.Mutex
	records  map[string][]byte
	accounts map[string]commitment.Update
	storage  map[string]commitment.Update
	discard  bool
}

func newFeedBenchContext(records map[string][]byte, discard bool, leaves []feedBenchLeaf) *feedBenchContext {
	ctx := &feedBenchContext{records: make(map[string][]byte, len(records)), accounts: make(map[string]commitment.Update), storage: make(map[string]commitment.Update), discard: discard}
	for key, value := range records {
		ctx.records[key] = bytes.Clone(value)
	}
	for i := range leaves {
		leaf := &leaves[i]
		if len(leaf.plain) == 20 {
			ctx.accounts[string(leaf.plain)] = leaf.state
		} else {
			ctx.storage[string(leaf.plain)] = leaf.state
		}
	}
	return ctx
}

func (c *feedBenchContext) Branch(key []byte) ([]byte, kv.Step, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *feedBenchContext) PutBranch(key, data, prev []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !bytes.Equal(c.records[string(key)], prev) {
		return &feedBenchError{key: bytes.Clone(key)}
	}
	if c.discard {
		return nil
	}
	if len(data) == 0 {
		delete(c.records, string(key))
	} else {
		c.records[string(key)] = bytes.Clone(data)
	}
	return nil
}

func (c *feedBenchContext) Account(key []byte) (*commitment.Update, error) {
	update := c.accounts[string(key)]
	return &update, nil
}

func (c *feedBenchContext) Storage(key []byte) (*commitment.Update, error) {
	update := c.storage[string(key)]
	return &update, nil
}

type feedBenchError struct{ key []byte }

func (e *feedBenchError) Error() string { return "benchmark previous record mismatch" }

type feedBenchReader struct{ values map[string][]byte }

func (r *feedBenchReader) WithHistory() bool { return false }

func (r *feedBenchReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *feedBenchReader) Read(domain kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	return bytes.Clone(r.values[feedBenchValueKey(domain, key)]), 0, nil
}

func (r *feedBenchReader) Clone(kv.TemporalTx) commitmentdb.StateReader { return r }

func (r *feedBenchReader) CloneForWorker(context.Context, kv.TemporalTx) commitmentdb.StateReader {
	return r
}

func feedBenchValueKey(domain kv.Domain, key []byte) string {
	return string(append([]byte{byte(domain)}, key...))
}

func feedBenchFixtureFor(kind string) feedBenchFixture {
	addressCount, slotsPerAddress := 5000, 1
	seedCount := addressCount
	switch kind {
	case "tip":
		addressCount, seedCount, slotsPerAddress = 5000, 1000, 0
	case "whale":
		addressCount, slotsPerAddress = 1, 512
		seedCount = addressCount
	case "rebuild":
		addressCount, slotsPerAddress = 5000, 0
		seedCount = 0
	default:
		panic(kind)
	}
	makeLeaves := func(from, to int, value byte) []feedBenchLeaf {
		leaves := make([]feedBenchLeaf, 0, (to-from)*(slotsPerAddress+1))
		for addressIndex := from; addressIndex < to; addressIndex++ {
			address := make([]byte, 20)
			address[18] = byte(addressIndex >> 8)
			address[19] = byte(addressIndex)
			balance := uint256.NewInt(1)
			basic, err := eip8297.EncodeBasicData(1, balance, 0)
			if err != nil {
				panic(err)
			}
			leaves = append(leaves, feedBenchLeaf{plain: bytes.Clone(address), key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), value: basic, state: commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate, Balance: *balance, Nonce: 1, CodeHash: empty.CodeHash}})
			for slotIndex := range slotsPerAddress {
				slot := make([]byte, 32)
				binary.BigEndian.PutUint64(slot[24:], uint64(addressIndex*slotsPerAddress+slotIndex))
				plain := append(bytes.Clone(address), slot...)
				var raw [eip8297.ValueLength]byte
				raw[len(raw)-1] = value
				leaves = append(leaves, feedBenchLeaf{plain: plain, key: eip8297.TreeKeyStorage(address, slot), value: eip8297.EncodeStorageValue(raw[:]), state: commitment.Update{Flags: commitment.StorageUpdate, StorageLen: eip8297.ValueLength, Storage: raw}})
			}
		}
		return leaves
	}
	batchFrom := 0
	seed := makeLeaves(0, seedCount, 1)
	batch := makeLeaves(batchFrom, batchFrom+addressCount, 2)
	if slotsPerAddress != 0 && kind != "rebuild" {
		batch = slices.DeleteFunc(batch, func(leaf feedBenchLeaf) bool { return len(leaf.plain) == 20 })
	}
	return feedBenchFixture{seed: seed, batch: batch}
}

func feedBenchReaderFor(fixture feedBenchFixture) *feedBenchReader {
	values := make(map[string][]byte)
	leaves := append(append([]feedBenchLeaf(nil), fixture.seed...), fixture.batch...)
	for i := range leaves {
		leaf := &leaves[i]
		if len(leaf.plain) == 20 {
			account := accounts.Account{Nonce: leaf.state.Nonce, Balance: leaf.state.Balance, CodeHash: accounts.EmptyCodeHash}
			values[feedBenchValueKey(kv.AccountsDomain, leaf.plain)] = accounts.SerialiseV3(&account)
		} else {
			values[feedBenchValueKey(kv.StorageDomain, leaf.plain)] = bytes.Clone(leaf.state.Storage[:])
		}
	}
	return &feedBenchReader{values: values}
}

func feedBenchOps(leaves []feedBenchLeaf) []pbt.Op {
	ops := make([]pbt.Op, 0, len(leaves)*2)
	for i := range leaves {
		leaf := &leaves[i]
		ops = append(ops, pbt.Op{Key: leaf.key, Value: leaf.value})
		if len(leaf.plain) == 20 {
			ops = append(ops, pbt.Op{Key: eip8297.TreeKeyAccount(leaf.plain, eip8297.CodeHashLeafKey), Value: eip8297.CodeHashValue(empty.CodeHash)})
		}
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	return ops
}

func feedBenchPlainKeys(leaves []feedBenchLeaf) map[string]struct{} {
	keys := make(map[string]struct{}, len(leaves))
	for i := range leaves {
		leaf := &leaves[i]
		keys[string(leaf.plain)] = struct{}{}
	}
	return keys
}

func feedBenchState(leaves ...[]feedBenchLeaf) []feedBenchLeaf {
	result := make([]feedBenchLeaf, 0)
	for _, group := range leaves {
		result = append(result, group...)
	}
	return result
}

func feedBenchRecords(records map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(records))
	for key, value := range records {
		result[key] = bytes.Clone(value)
	}
	return result
}

func feedBenchKeyOnlyUpdates(tb testing.TB, leaves []feedBenchLeaf) *commitment.Updates {
	tb.Helper()
	previous := commitment.PBinHashSuiteName()
	require.NoError(tb, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	updates := commitment.NewBinUpdates(tb.TempDir(), feedBenchPlainKeys(leaves))
	require.NoError(tb, commitment.SetPBinHashSuite(previous))
	return updates
}

func feedBenchRun(b *testing.B, fixture feedBenchFixture, workers int, expectParallel bool) {
	seedOps := feedBenchOps(fixture.seed)
	newSeed := newFeedBenchContext(nil, false, fixture.seed)
	newSeedRoot, err := pbt.NewTrie(newSeed).Process(seedOps)
	require.NoError(b, err)
	newBase := feedBenchRecords(newSeed.records)
	oldSeed := newFeedBenchContext(nil, false, fixture.seed)
	oldTrie := commitment.NewPBinPatriciaHashed(oldSeed)
	require.NoError(b, oldTrie.SetPBinHashSuite(commitment.PBinHashBlake3))
	seedUpdates := feedBenchKeyOnlyUpdates(b, fixture.seed)
	oldSeedRoot, err := oldTrie.Process(context.Background(), seedUpdates, "benchmark", nil, commitment.WarmupConfig{})
	require.NoError(b, err)
	require.Equal(b, oldSeedRoot, newSeedRoot[:])
	seedUpdates.Close()
	oldBase := feedBenchRecords(oldSeed.records)
	reader := feedBenchReaderFor(fixture)
	keys := feedBenchPlainKeys(fixture.batch)
	state := feedBenchState(fixture.seed, fixture.batch)
	var newDurations, oldDurations []time.Duration
	for round := range 5 {
		newContext := newFeedBenchContext(newBase, true, state)
		newTrie := pbt.NewTrie(newContext)
		newTrie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) {
			return newFeedBenchContext(newBase, true, state), func() {}
		})
		var active, peak atomic.Int32
		newTrie.SetCoreActivityHook(func(start bool) {
			if !start {
				active.Add(-1)
				return
			}
			current := active.Add(1)
			for {
				old := peak.Load()
				if old >= current || peak.CompareAndSwap(old, current) {
					break
				}
			}
			runtime.Gosched()
		})
		oldContext := newFeedBenchContext(oldBase, true, state)
		oldTrie := commitment.NewPBinPatriciaHashed(oldContext)
		require.NoError(b, oldTrie.SetPBinHashSuite(commitment.PBinHashBlake3))
		oldUpdates := feedBenchKeyOnlyUpdates(b, fixture.batch)
		if round%2 == 0 {
			before := benchmarkUptime()
			start := time.Now()
			feed, feedErr := commitmentdb.BinFeedFromState(keys, nil, nil, reader)
			require.NoError(b, feedErr)
			ops, translateErr := pbt.TranslateFeed(feed)
			require.NoError(b, translateErr)
			newRoot, processErr := newTrie.ProcessParallelContext(context.Background(), ops, workers)
			require.NoError(b, processErr)
			newDurations = append(newDurations, time.Since(start))
			after := benchmarkUptime()
			b.Logf("new round=%d uptime-before=%s uptime-after=%s peak=%d", round, before, after, peak.Load())
			if workers > 1 && expectParallel {
				require.Greater(b, peak.Load(), int32(1))
			}
			before = benchmarkUptime()
			start = time.Now()
			oldRoot, processErr := oldTrie.Process(context.Background(), oldUpdates, "benchmark", nil, commitment.WarmupConfig{})
			oldDurations = append(oldDurations, time.Since(start))
			after = benchmarkUptime()
			require.NoError(b, processErr)
			b.Logf("old round=%d uptime-before=%s uptime-after=%s", round, before, after)
			require.Equal(b, oldRoot, newRoot[:])
		} else {
			before := benchmarkUptime()
			start := time.Now()
			oldRoot, processErr := oldTrie.Process(context.Background(), oldUpdates, "benchmark", nil, commitment.WarmupConfig{})
			oldDurations = append(oldDurations, time.Since(start))
			after := benchmarkUptime()
			require.NoError(b, processErr)
			b.Logf("old round=%d uptime-before=%s uptime-after=%s", round, before, after)
			before = benchmarkUptime()
			start = time.Now()
			feed, feedErr := commitmentdb.BinFeedFromState(keys, nil, nil, reader)
			require.NoError(b, feedErr)
			ops, translateErr := pbt.TranslateFeed(feed)
			require.NoError(b, translateErr)
			newRoot, processErr := newTrie.ProcessParallelContext(context.Background(), ops, workers)
			newDurations = append(newDurations, time.Since(start))
			after = benchmarkUptime()
			require.NoError(b, processErr)
			b.Logf("new round=%d uptime-before=%s uptime-after=%s peak=%d", round, before, after, peak.Load())
			if workers > 1 && expectParallel {
				require.Greater(b, peak.Load(), int32(1))
			}
			require.Equal(b, oldRoot, newRoot[:])
		}
		oldUpdates.Close()
		oldTrie.Release()
	}
	newMin, oldMin := newDurations[0], oldDurations[0]
	for i := 1; i < len(newDurations); i++ {
		if newDurations[i] < newMin {
			newMin = newDurations[i]
		}
		if oldDurations[i] < oldMin {
			oldMin = oldDurations[i]
		}
	}
	keysCount := float64(len(fixture.batch))
	b.ReportMetric(float64(newMin.Microseconds())/keysCount, "new-ms/1k")
	b.ReportMetric(float64(oldMin.Microseconds())/keysCount, "old-ms/1k")
}

func BenchmarkPBinCompareFeedTip(b *testing.B) {
	previous := eip8297.HashSuiteName()
	previousPBin := commitment.PBinHashSuiteName()
	b.Cleanup(func() { require.NoError(b, eip8297.SetHashSuite(previous)) })
	b.Cleanup(func() { require.NoError(b, commitment.SetPBinHashSuite(previousPBin)) })
	require.NoError(b, eip8297.SetHashSuite(eip8297.HashBlake3))
	require.NoError(b, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	fixture := feedBenchFixtureFor("tip")
	for _, workers := range []int{1, physicalCoreCount()} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) { feedBenchRun(b, fixture, workers, true) })
	}
}

func BenchmarkPBinCompareFeedWhale(b *testing.B) {
	previous := eip8297.HashSuiteName()
	previousPBin := commitment.PBinHashSuiteName()
	b.Cleanup(func() { require.NoError(b, eip8297.SetHashSuite(previous)) })
	b.Cleanup(func() { require.NoError(b, commitment.SetPBinHashSuite(previousPBin)) })
	require.NoError(b, eip8297.SetHashSuite(eip8297.HashBlake3))
	require.NoError(b, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	fixture := feedBenchFixtureFor("whale")
	for _, workers := range []int{1, physicalCoreCount()} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) { feedBenchRun(b, fixture, workers, true) })
	}
}

func BenchmarkPBinCompareFeedRebuild(b *testing.B) {
	previous := eip8297.HashSuiteName()
	previousPBin := commitment.PBinHashSuiteName()
	b.Cleanup(func() { require.NoError(b, eip8297.SetHashSuite(previous)) })
	b.Cleanup(func() { require.NoError(b, commitment.SetPBinHashSuite(previousPBin)) })
	require.NoError(b, eip8297.SetHashSuite(eip8297.HashBlake3))
	require.NoError(b, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	fixture := feedBenchFixtureFor("rebuild")
	for _, workers := range []int{1, physicalCoreCount()} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) { feedBenchRun(b, fixture, workers, true) })
	}
}

func physicalCoreCount() int {
	if output, err := exec.CommandContext(context.Background(), "sysctl", "-n", "hw.physicalcpu").Output(); err == nil {
		if count, err := strconv.Atoi(strings.TrimSpace(string(output))); err == nil && count > 0 {
			return count
		}
	}
	return runtime.NumCPU()
}

func benchmarkUptime() string {
	output, err := exec.CommandContext(context.Background(), "uptime").Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(output))
}
