// Copyright 2025 The Erigon Authors
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

package tools

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/stretchr/testify/require"
)

func TestPreStateHasher_Determinism(t *testing.T) {
	h1 := GetPreStateHasher()
	defer PutPreStateHasher(h1)
	h1.Reset()

	h1.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xAA})
	h1.AddRead(kv.StorageDomain, []byte{0x02}, []byte{0xBB})
	hash1 := h1.Finalize()

	h2 := GetPreStateHasher()
	defer PutPreStateHasher(h2)
	h2.Reset()

	// Same reads, same order
	h2.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xAA})
	h2.AddRead(kv.StorageDomain, []byte{0x02}, []byte{0xBB})
	hash2 := h2.Finalize()

	require.Equal(t, hash1, hash2, "same reads must produce same hash")
	require.NotEqual(t, common.Hash{}, hash1, "hash must be non-zero")
}

func TestPreStateHasher_OrderIndependent(t *testing.T) {
	h1 := GetPreStateHasher()
	defer PutPreStateHasher(h1)
	h1.Reset()

	h1.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xAA})
	h1.AddRead(kv.StorageDomain, []byte{0x02}, []byte{0xBB})
	hash1 := h1.Finalize()

	h2 := GetPreStateHasher()
	defer PutPreStateHasher(h2)
	h2.Reset()

	// Same reads, reversed order
	h2.AddRead(kv.StorageDomain, []byte{0x02}, []byte{0xBB})
	h2.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xAA})
	hash2 := h2.Finalize()

	require.Equal(t, hash1, hash2, "read order must not affect hash")
}

func TestPreStateHasher_DifferentReads(t *testing.T) {
	h1 := GetPreStateHasher()
	defer PutPreStateHasher(h1)
	h1.Reset()
	h1.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xAA})
	hash1 := h1.Finalize()

	h2 := GetPreStateHasher()
	defer PutPreStateHasher(h2)
	h2.Reset()
	h2.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xBB}) // different value
	hash2 := h2.Finalize()

	require.NotEqual(t, hash1, hash2, "different values must produce different hashes")
}

func TestPreStateHasher_DifferentKeys(t *testing.T) {
	h1 := GetPreStateHasher()
	defer PutPreStateHasher(h1)
	h1.Reset()
	h1.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xAA})
	hash1 := h1.Finalize()

	h2 := GetPreStateHasher()
	defer PutPreStateHasher(h2)
	h2.Reset()
	h2.AddRead(kv.AccountsDomain, []byte{0x02}, []byte{0xAA}) // different key
	hash2 := h2.Finalize()

	require.NotEqual(t, hash1, hash2, "different keys must produce different hashes")
}

func TestPreStateHasher_DifferentDomains(t *testing.T) {
	h1 := GetPreStateHasher()
	defer PutPreStateHasher(h1)
	h1.Reset()
	h1.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xAA})
	hash1 := h1.Finalize()

	h2 := GetPreStateHasher()
	defer PutPreStateHasher(h2)
	h2.Reset()
	h2.AddRead(kv.StorageDomain, []byte{0x01}, []byte{0xAA}) // different domain
	hash2 := h2.Finalize()

	require.NotEqual(t, hash1, hash2, "different domains must produce different hashes")
}

func TestPreStateHasher_Empty(t *testing.T) {
	h := GetPreStateHasher()
	defer PutPreStateHasher(h)
	h.Reset()
	hash := h.Finalize()

	require.NotEqual(t, common.Hash{}, hash, "empty reads must produce non-zero hash (keccak of empty)")
}

func TestPreStateHasher_Reset(t *testing.T) {
	h := GetPreStateHasher()
	defer PutPreStateHasher(h)

	h.Reset()
	h.AddRead(kv.AccountsDomain, []byte{0x01}, []byte{0xAA})
	hash1 := h.Finalize()

	// Reset and add different read
	h.Reset()
	h.AddRead(kv.AccountsDomain, []byte{0x02}, []byte{0xBB})
	hash2 := h.Finalize()

	require.NotEqual(t, hash1, hash2, "reset must clear previous reads")
}

func TestPreStateHasher_MatchesHashChanges(t *testing.T) {
	// The PreStateHasher should produce the same hash as hashChanges
	// for the same set of (domain, key, value) tuples.
	reads := []StateChange{
		{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}},
		{Domain: kv.StorageDomain, Key: []byte{0x02}, Value: []byte{0xBB}},
	}

	// Use hashChanges directly (sorts internally via sortChanges in NewStateEntry)
	sorted := make([]StateChange, len(reads))
	copy(sorted, reads)
	sortChanges(sorted)
	expected := hashChanges(sorted)

	// Use PreStateHasher
	h := GetPreStateHasher()
	defer PutPreStateHasher(h)
	h.Reset()
	for _, r := range reads {
		h.AddRead(r.Domain, r.Key, r.Value)
	}
	actual := h.Finalize()

	require.Equal(t, expected, actual, "PreStateHasher must match hashChanges for same data")
}
