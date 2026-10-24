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
	"math/rand"
	"slices"
	"testing"

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
						require.NoError(t, parallel.Add(entry.Key, entry.Value))
					}
					got, err := parallel.RootHash()
					require.NoError(t, err)
					require.Equal(t, want, got, "workers=%d corpus=%d", workers, corpusIndex)
				}
			}
		})
	}
}

func parallelStreamRootCorpora() [][]Entry {
	return [][]Entry{
		nil,
		parallelStreamRootAccountEntries(1),
		parallelStreamRootAccountEntries(2),
		parallelStreamRootZoneEntries(),
		parallelStreamRootPrefixEntries(),
	}
}

func parallelStreamRootAccountEntries(count int) []Entry {
	entries := make([]Entry, count)
	for i := range entries {
		position := make([]byte, 32)
		position[0] = 0x12
		position[31] = byte(i + 1)
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

func parallelStreamRootPrefixEntries() []Entry {
	entries := make([]Entry, 8)
	for i := range entries {
		position := make([]byte, 32)
		position[0] = 0x12
		position[1] = 0x30
		position[31] = byte(i)
		entries[i] = Entry{Key: TreeKey(AccountZone, position, byte(i)), Value: streamRootValue(byte(i + 1))}
	}
	return entries
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
