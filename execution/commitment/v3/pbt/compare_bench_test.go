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

package pbt

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type compareLeaf struct {
	plain  []byte
	key    []byte
	value  [eip8297.ValueLength]byte
	raw    [eip8297.ValueLength]byte
	update commitment.Update
}

type compareFixture struct {
	seed  []compareLeaf
	batch []compareLeaf
}

type compareContext struct {
	*trieTestContext
	discard bool
	state   map[string]commitment.Update
}

func newCompareContext(records map[string][]byte, discard bool, stateLeaves []compareLeaf) *compareContext {
	ctx := newTrieTestContext()
	for key, value := range records {
		ctx.records[key] = bytes.Clone(value)
	}
	state := make(map[string]commitment.Update, len(stateLeaves))
	for i := range stateLeaves {
		state[string(stateLeaves[i].plain)] = stateLeaves[i].update
	}
	return &compareContext{trieTestContext: ctx, discard: discard, state: state}
}

func (c *compareContext) Storage(key []byte) (*commitment.Update, error) {
	update, ok := c.state[string(key)]
	if !ok {
		return nil, fmt.Errorf("unexpected storage read for %x", key)
	}
	return &update, nil
}

func (c *compareContext) PutBranch(key, data, prev []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !bytes.Equal(c.records[string(key)], prev) {
		return fmt.Errorf("previous record mismatch for %x", key)
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

func compareStorageLeaf(index, addressCount int, value byte, hasher eip8297.KeyHasherFunc) compareLeaf {
	address := make([]byte, 20)
	binary.BigEndian.PutUint64(address[12:], uint64(index%addressCount+1))
	slot := make([]byte, 32)
	binary.BigEndian.PutUint64(slot[24:], uint64(index))
	plain := append(bytes.Clone(address), slot...)
	var rawValue [eip8297.ValueLength]byte
	rawValue[len(rawValue)-1] = value
	update := commitment.Update{Flags: commitment.StorageUpdate, StorageLen: eip8297.ValueLength}
	update.Storage = rawValue
	key := hasher(plain)
	return compareLeaf{plain: plain, key: key, value: eip8297.EncodeStorageValue(rawValue[:]), raw: rawValue, update: update}
}

func compareFixtureFor(kind string, hasher eip8297.KeyHasherFunc) compareFixture {
	switch kind {
	case "tip":
		seed := make([]compareLeaf, 10_000)
		for i := range seed {
			seed[i] = compareStorageLeaf(i, 10_000, 1, hasher)
		}
		batch := make([]compareLeaf, 5_000)
		copy(batch, seed[5_000:])
		for i := range batch {
			batch[i].raw[len(batch[i].raw)-1] = 2
			batch[i].value = eip8297.EncodeStorageValue(batch[i].raw[:])
			batch[i].update.Storage = batch[i].raw
		}
		return compareFixture{seed: seed, batch: batch}
	case "whale":
		seed := make([]compareLeaf, 5_000)
		batch := make([]compareLeaf, 5_000)
		for i := range seed {
			seed[i] = compareStorageLeaf(i+64, 1, 1, hasher)
			batch[i] = compareStorageLeaf(i+64, 1, 2, hasher)
		}
		return compareFixture{seed: seed, batch: batch}
	case "rebuild":
		batch := make([]compareLeaf, 5_000)
		for i := range batch {
			batch[i] = compareStorageLeaf(i, 5_000, 3, hasher)
		}
		return compareFixture{batch: batch}
	default:
		panic("unknown benchmark fixture")
	}
}

func compareOps(leaves []compareLeaf) []Op {
	ops := make([]Op, len(leaves))
	for i := range leaves {
		ops[i] = Op{Key: bytes.Clone(leaves[i].key), Value: leaves[i].value}
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	return ops
}

func compareUpdates(leaves []compareLeaf, tmpdir string) *commitment.Updates {
	hasher := eip8297.KeyHasherWith(eip8297.SelectedHash())
	updates := commitment.NewUpdates(commitment.ModeUpdate, tmpdir, func(key []byte) []byte { return hasher(key) })
	for i := range leaves {
		update := leaves[i].update
		updates.TouchPlainKeyDirect(string(leaves[i].plain), &update)
	}
	return updates
}

func compareRecords(records map[string][]byte) map[string][]byte {
	copyRecords := make(map[string][]byte, len(records))
	for key, value := range records {
		copyRecords[key] = bytes.Clone(value)
	}
	return copyRecords
}

func prepareCompareFixture(b *testing.B, fixture compareFixture) (map[string][]byte, map[string][]byte) {
	b.Helper()
	seedOps := compareOps(fixture.seed)
	previous := eip8297.HashSuiteName()
	b.Cleanup(func() { require.NoError(b, eip8297.SetHashSuite(previous)) })
	require.NoError(b, eip8297.SetHashSuite(eip8297.HashBlake3))
	require.NoError(b, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))

	newContext := newCompareContext(nil, false, nil)
	newRoot, err := NewTrie(newContext).Process(seedOps)
	require.NoError(b, err)

	oldContext := newCompareContext(nil, false, fixture.seed)
	oldTrie := commitment.NewPBinPatriciaHashed(oldContext)
	require.NoError(b, oldTrie.SetPBinHashSuite(commitment.PBinHashBlake3))
	oldUpdates := compareUpdates(fixture.seed, b.TempDir())
	oldRoot, err := oldTrie.Process(context.Background(), oldUpdates, "benchmark", nil, commitment.WarmupConfig{})
	require.NoError(b, err)
	oldUpdates.Close()
	oldTrie.Release()
	require.Equal(b, oldRoot, newRoot[:])
	return compareRecords(newContext.records), compareRecords(oldContext.records)
}

func compareRun(b *testing.B, fixture compareFixture, newBase, oldBase map[string][]byte, workers int) {
	b.Helper()
	newOps := compareOps(fixture.batch)
	rounds := 5
	newDurations := make([]time.Duration, 0, rounds)
	oldDurations := make([]time.Duration, 0, rounds)
	for round := range rounds {
		newContext := newCompareContext(newBase, true, nil)
		newTrie := NewTrie(newContext)
		oldContext := newCompareContext(oldBase, true, fixture.seed)
		oldTrie := commitment.NewPBinPatriciaHashed(oldContext)
		require.NoError(b, oldTrie.SetPBinHashSuite(commitment.PBinHashBlake3))
		oldUpdates := compareUpdates(fixture.batch, b.TempDir())
		if round%2 == 0 {
			before := benchmarkUptime()
			start := time.Now()
			newRoot, err := newTrie.ProcessParallelContext(context.Background(), newOps, workers)
			newDurations = append(newDurations, time.Since(start))
			after := benchmarkUptime()
			require.NoError(b, err)
			b.Logf("new round=%d uptime-before=%s uptime-after=%s", round, before, after)

			before = benchmarkUptime()
			start = time.Now()
			oldRoot, err := oldTrie.Process(context.Background(), oldUpdates, "benchmark", nil, commitment.WarmupConfig{})
			oldDurations = append(oldDurations, time.Since(start))
			after = benchmarkUptime()
			require.NoError(b, err)
			b.Logf("old round=%d uptime-before=%s uptime-after=%s", round, before, after)
			require.Equal(b, oldRoot, newRoot[:])
		} else {
			before := benchmarkUptime()
			start := time.Now()
			oldRoot, err := oldTrie.Process(context.Background(), oldUpdates, "benchmark", nil, commitment.WarmupConfig{})
			oldDurations = append(oldDurations, time.Since(start))
			after := benchmarkUptime()
			require.NoError(b, err)
			b.Logf("old round=%d uptime-before=%s uptime-after=%s", round, before, after)

			before = benchmarkUptime()
			start = time.Now()
			newRoot, err := newTrie.ProcessParallelContext(context.Background(), newOps, workers)
			newDurations = append(newDurations, time.Since(start))
			after = benchmarkUptime()
			require.NoError(b, err)
			b.Logf("new round=%d uptime-before=%s uptime-after=%s", round, before, after)
			require.Equal(b, oldRoot, newRoot[:])
		}
		oldUpdates.Close()
		oldTrie.Release()
	}
	newMin, oldMin := newDurations[0], oldDurations[0]
	for i := 1; i < rounds; i++ {
		newMin = min(newMin, newDurations[i])
		oldMin = min(oldMin, oldDurations[i])
	}
	keys := float64(len(fixture.batch))
	b.ReportMetric(float64(newMin.Microseconds())/1000*1000/keys, "new-ms/1k")
	b.ReportMetric(float64(oldMin.Microseconds())/1000*1000/keys, "old-ms/1k")
}

func benchmarkUptime() string {
	output, err := exec.CommandContext(context.Background(), "uptime").Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(output))
}

func physicalCoreCount() int {
	if output, err := exec.CommandContext(context.Background(), "sysctl", "-n", "hw.physicalcpu").Output(); err == nil {
		if count, err := strconv.Atoi(strings.TrimSpace(string(output))); err == nil && count > 0 {
			return count
		}
	}
	if output, err := exec.CommandContext(context.Background(), "system_profiler", "SPHardwareDataType").Output(); err == nil {
		for line := range strings.SplitSeq(string(output), "\n") {
			if strings.Contains(line, "Total Number of Cores") {
				parts := strings.Split(line, ":")
				if len(parts) == 2 {
					if count, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil && count > 0 {
						return count
					}
				}
			}
		}
	}
	return runtime.NumCPU()
}

func BenchmarkPBinCompareTip(b *testing.B) {
	benchmarkPBinFixture(b, "tip")
}

func BenchmarkPBinCompareWhale(b *testing.B) {
	benchmarkPBinFixture(b, "whale")
}

func BenchmarkPBinCompareRebuild(b *testing.B) {
	benchmarkPBinFixture(b, "rebuild")
}

func benchmarkPBinFixture(b *testing.B, kind string) {
	fixture := benchmarkFixture(b, kind)
	newBase, oldBase := prepareCompareFixture(b, fixture)
	for _, workers := range []int{1, physicalCoreCount()} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) {
			compareRun(b, fixture, newBase, oldBase, workers)
		})
	}
}

func benchmarkFixture(b *testing.B, kind string) compareFixture {
	previous := eip8297.HashSuiteName()
	require.NoError(b, eip8297.SetHashSuite(eip8297.HashBlake3))
	b.Cleanup(func() { require.NoError(b, eip8297.SetHashSuite(previous)) })
	return compareFixtureFor(kind, eip8297.KeyHasherWith(eip8297.SelectedHash()))
}

func BenchmarkPBinDensity(b *testing.B) {
	fixture := benchmarkFixture(b, "whale")
	newBase, oldBase := prepareCompareFixture(b, fixture)
	newDensity := pbtDensity(newBase, fixture.seed)
	oldDensity, err := commitment.PBinLegacyRecordDensity(oldBase)
	require.NoError(b, err)
	b.ReportMetric(float64(newDensity.rows)/float64(len(fixture.batch)), "new-rows/leaf")
	b.ReportMetric(float64(oldDensity.Rows)/float64(len(fixture.batch)), "old-rows/leaf")
	b.ReportMetric(float64(newDensity.cells)/float64(len(fixture.batch)), "new-cells/leaf")
	b.ReportMetric(float64(oldDensity.Cells)/float64(len(fixture.batch)), "old-cells/leaf")
	b.ReportMetric(float64(newDensity.bucketRecords), "new-bucket-records")
	b.ReportMetric(0, "old-bucket-records")
	b.ReportMetric(float64(newDensity.bytes)/float64(len(fixture.batch)), "new-bytes/leaf")
	b.ReportMetric(float64(oldDensity.Bytes)/float64(len(fixture.batch)), "old-bytes/leaf")
	b.ReportMetric(float64(newDensity.bytes), "new-kv-size")
	b.ReportMetric(float64(oldDensity.Bytes), "old-kv-size")
	windows := make([]int, 0, len(oldDensity.Splits))
	for window := range oldDensity.Splits {
		windows = append(windows, window)
	}
	sort.Ints(windows)
	for _, window := range windows {
		b.ReportMetric(float64(oldDensity.Splits[window]), fmt.Sprintf("old-splits/w%d", window))
	}
}

type compareDensity struct {
	rows, cells, bytes, bucketRecords int
}

func pbtDensity(records map[string][]byte, leaves []compareLeaf) compareDensity {
	density := compareDensity{}
	bucketKeys := make(map[string]struct{}, len(leaves))
	for i := range leaves {
		key, err := bucketKeyForStorage(leaves[i].key)
		if err == nil {
			bucketKeys[string(key)] = struct{}{}
		}
	}
	for key, value := range records {
		if len(value) == 0 {
			continue
		}
		density.bytes += len(key) + len(value)
		if _, ok := bucketKeys[key]; ok {
			density.bucketRecords++
			continue
		}
		record, err := DecodeRecord([]byte(key), value)
		if err != nil {
			panic(err)
		}
		if record.Form == RowRoot {
			density.rows++
		}
		for i := range record.Cells {
			if record.Cells[i].Kind != EmptyCell {
				density.cells++
			}
		}
	}
	return density
}
