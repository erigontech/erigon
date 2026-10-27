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

package eip8297

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParallelStreamRootMatchesSerial(t *testing.T) {
	for _, suite := range []string{HashKeccak, HashBlake3} {
		t.Run(suite, func(t *testing.T) {
			previous := HashSuiteName()
			t.Cleanup(func() { require.NoError(t, SetHashSuite(previous)) })
			require.NoError(t, SetHashSuite(suite))
			sum := streamRootHash(suite)
			corpora := parallelStreamRootCorpora()
			rnd := rand.New(rand.NewSource(0x8297))
			for range 10_000 {
				corpora = append(corpora, randomParallelStreamRootEntries(rnd))
			}
			for corpusIndex, entries := range corpora {
				serial, err := NewStreamRootBuilder(sum)
				require.NoError(t, err)
				for _, entry := range entries {
					require.NoError(t, serial.Add(entry.Key, entry.Value))
				}
				want, err := serial.RootHash()
				require.NoError(t, err)
				for _, workers := range []int{1, 3, 16} {
					parallel, err := NewParallelStreamRoot(sum, workers)
					require.NoError(t, err)
					for _, entry := range entries {
						require.NoError(t, parallel.AddContext(t.Context(), entry.Key, entry.Value))
					}
					got, err := parallel.RootHash()
					require.NoError(t, err)
					require.Equal(t, want, got, "workers=%d corpus=%d", workers, corpusIndex)
				}
			}
		})
	}
}

func TestParallelStreamRootBuildsShardsConcurrently(t *testing.T) {
	for _, test := range []struct {
		name          string
		zone          byte
		positionBytes int
	}{
		{name: "account", zone: AccountZone, positionBytes: 32},
		{name: "storage", zone: StorageZone, positionBytes: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			sum := streamRootHash(HashKeccak)
			parallel, err := NewParallelStreamRoot(sum, 4)
			require.NoError(t, err)
			var active, maxActive atomic.Int32
			parallel.buildActive = &active
			parallel.buildMax = &maxActive
			release := make(chan struct{})
			var first atomic.Bool
			parallel.buildStart = func() {
				if first.CompareAndSwap(false, true) {
					<-release
				}
			}
			entries := make([]Entry, 0, 48)
			for shard := range 16 {
				for leaf := range 3 {
					position := make([]byte, test.positionBytes)
					position[0] = byte(shard << 4)
					position[test.positionBytes-1] = byte(leaf + 1)
					entries = append(entries, Entry{Key: TreeKey(test.zone, position, byte(leaf)), Value: streamRootValue(byte(leaf + 1))})
				}
			}
			slices.SortFunc(entries, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
			done := make(chan error, 1)
			go func() {
				for _, entry := range entries {
					if err := parallel.AddContext(t.Context(), entry.Key, entry.Value); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			deadline := time.Now().Add(time.Second)
			for maxActive.Load() < 2 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			close(release)
			require.NoError(t, <-done)
			_, err = parallel.RootHash()
			require.NoError(t, err)
			require.GreaterOrEqual(t, maxActive.Load(), int32(2), "sorted stream must build at least two shards concurrently")
		})
	}
}

func TestParallelStreamRootProducerReachesNextShardBeforeFirstFinishes(t *testing.T) {
	parallel, err := NewParallelStreamRoot(streamRootHash(HashKeccak), 2)
	require.NoError(t, err)
	var active, maxActive atomic.Int32
	parallel.buildActive = &active
	parallel.buildMax = &maxActive
	release := make(chan struct{})
	var first atomic.Bool
	parallel.buildStart = func() {
		if first.CompareAndSwap(false, true) {
			<-release
		}
	}
	entries := parallelStreamRootLongShardEntries(10)
	done := make(chan error, 1)
	go func() {
		for _, entry := range entries {
			if err := parallel.AddContext(t.Context(), entry.Key, entry.Value); err != nil {
				done <- err
				return
			}
		}
		_, err := parallel.RootHash()
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	reached := false
	for time.Now().Before(deadline) {
		if maxActive.Load() >= 2 {
			reached = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	require.True(t, reached, "producer must reach the second shard while the first shard is still building")
	require.NoError(t, <-done)
}

func TestParallelStreamRootBoundsSkewedResidentBytes(t *testing.T) {
	entries := parallelStreamRootSkewedEntries(100_000)
	serial, err := NewStreamRootBuilder(streamRootHash(HashKeccak))
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(t, serial.Add(entry.Key, entry.Value))
	}
	want, err := serial.RootHash()
	require.NoError(t, err)
	parallel, err := newParallelStreamRoot(streamRootHash(HashKeccak), 4, 2*parallelStreamRootBatchResidentBytes)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(t, parallel.AddContext(t.Context(), entry.Key, entry.Value))
	}
	got, err := parallel.RootHash()
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.LessOrEqual(t, parallel.peakResident.Load(), parallel.residentLimit)
}

func parallelStreamRootSkewedEntries(count int) []Entry {
	entries := make([]Entry, count)
	for i := range entries {
		key := make([]byte, StorageKeyLength)
		key[0] = StorageZone
		binary.BigEndian.PutUint64(key[StorageKeyLength-8:], uint64(i))
		value := streamRootValue(byte(i))
		value[0] = 1
		entries[i] = Entry{Key: key, Value: value}
	}
	return entries
}

func parallelStreamRootLongShardEntries(batches int) []Entry {
	entryBytes := StorageKeyLength + ValueLength
	count := batches*parallelStreamRootBatchBytes/entryBytes + 1
	entries := make([]Entry, count+1)
	for i := range count {
		key := make([]byte, StorageKeyLength)
		key[0] = StorageZone
		binary.BigEndian.PutUint64(key[StorageKeyLength-8:], uint64(i))
		value := streamRootValue(byte(i))
		value[0] = 1
		entries[i] = Entry{Key: key, Value: value}
	}
	key := make([]byte, StorageKeyLength)
	key[0] = StorageZone
	key[1] = 0x10
	value := streamRootValue(1)
	value[0] = 1
	entries[count] = Entry{Key: key, Value: value}
	return entries
}

func parallelStreamRootCorpora() [][]Entry {
	return [][]Entry{
		nil,
		parallelStreamRootEntries(1, nil, 1),
		parallelStreamRootEntries(2, nil, 1),
		parallelStreamRootZoneEntries(),
		parallelStreamRootEntries(8, []byte{0x12, 0x30}, 0),
	}
}

func parallelStreamRootEntries(count int, prefix []byte, positionStart byte) []Entry {
	entries := make([]Entry, count)
	for i := range entries {
		position := make([]byte, 32)
		if len(prefix) == 0 {
			prefix = []byte{0x12}
		}
		copy(position, prefix)
		position[31] = byte(i) + positionStart
		entries[i] = Entry{Key: TreeKey(AccountZone, position, byte(i)), Value: streamRootValue(byte(i + 1))}
	}
	return entries
}

func parallelStreamRootZoneEntries() []Entry {
	account := TreeKey(AccountZone, bytes.Repeat([]byte{0x12}, 32), 0)
	code := TreeKey(CodeZone, bytes.Repeat([]byte{0x34}, 32), 0)
	storage := TreeKey(StorageZone, bytes.Repeat([]byte{0x56}, 64), 0)
	entries := []Entry{{Key: account, Value: streamRootValue(1)}, {Key: code, Value: streamRootValue(2)}, {Key: storage, Value: streamRootValue(3)}}
	return sortStreamEntries(entries)
}

func randomParallelStreamRootEntries(rnd *rand.Rand) []Entry {
	count := rnd.Intn(32)
	entries := make([]Entry, 0, count)
	seen := make(map[string]struct{}, count)
	for len(entries) < count {
		zone := []byte{AccountZone, CodeZone, StorageZone}[rnd.Intn(3)]
		keyLength := 34
		if zone == StorageZone {
			keyLength = StorageKeyLength
		}
		key := make([]byte, keyLength)
		key[0] = zone
		_, _ = rnd.Read(key[1:])
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		value := make([]byte, ValueLength)
		_, _ = rnd.Read(value)
		if bytes.Equal(value, make([]byte, ValueLength)) {
			value[0] = 1
		}
		entries = append(entries, Entry{Key: key, Value: value})
	}
	slices.SortFunc(entries, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
	return entries
}

func TestParallelRejectsCrossShardPrefix(t *testing.T) {
	serial, err := NewStreamRootBuilder(streamRootHash(HashKeccak))
	require.NoError(t, err)
	parallel, err := NewParallelStreamRoot(streamRootHash(HashKeccak), 2)
	require.NoError(t, err)
	value := streamRootValue(1)
	require.NoError(t, serial.Add([]byte{0x12}, value))
	require.ErrorContains(t, serial.Add([]byte{0x12, 0x80}, value), "not prefix-free")
	require.NoError(t, parallel.AddContext(t.Context(), []byte{0x12}, value))
	require.NoError(t, parallel.AddContext(t.Context(), []byte{0x12, 0x80}, value))
	root, err := parallel.RootHash()
	require.Error(t, err, "parallel accepted prefix-related keys 12 and 1280: root=%x, err=%v", root, err)
}

func TestParallelBufferReuse(t *testing.T) {
	entries := parallelStreamRootEntries(2, nil, 1)
	sum := streamRootHash(HashKeccak)
	serial, err := NewStreamRootBuilder(sum)
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, serial.Add(e.Key, e.Value))
	}
	want, err := serial.RootHash()
	require.NoError(t, err)
	parallel, err := NewParallelStreamRoot(sum, 1)
	require.NoError(t, err)
	require.NoError(t, parallel.AddContext(t.Context(), entries[0].Key, entries[0].Value))
	require.NoError(t, parallel.AddContext(t.Context(), entries[1].Key, entries[1].Value))
	entries[1].Value[0] = 0x7f
	got, err := parallel.RootHash()
	require.NoError(t, err)
	require.Equal(t, want, got, "root changed after reuse of a buffer passed to Add")
}

func TestParallelStreamRootWorkerErrorReachesProducer(t *testing.T) {
	builder, err := NewParallelStreamRoot(streamRootHash(HashKeccak), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	key := []byte{0, 0}
	value := streamRootValue(1)
	if err := builder.AddContext(ctx, key, value); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddContext(ctx, key, value); err != nil {
		t.Fatal(err)
	}
	for shard := 1; shard < 10; shard++ {
		err = builder.AddContext(ctx, []byte{0, byte(shard << 4)}, value)
		if err != nil {
			break
		}
	}
	_, rootErr := builder.RootHash()
	if err == nil || !strings.Contains(err.Error(), "strictly ascending") {
		t.Fatalf("worker ordering error must reach producer; AddContext=%v, RootHash=%v", err, rootErr)
	}
}
