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

package commitment

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

// pbinTestStoredTree runs a small corpus through the engine and returns the
// state it persisted plus the root it computed.
func pbinTestStoredTree(t *testing.T) (*MockState, []byte) {
	t.Helper()
	corpus := new(pbinTestCorpus).
		storage(pbinOracleAddr(1), pbinOracleSlot(64), 0x01).
		storage(pbinOracleAddr(1), pbinOracleSlot(1000), 0x02, 0x03)
	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(corpus.plainKeys, corpus.updates))
	return ms, pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)
}

// Every record a Process run writes, the root record included, must survive a
// round-trip through the real TblCommitmentVals table. Domain iteration treats a
// zero-length key as end-of-stream and the empty key sorts first, so a root
// record stored under it truncates the iteration and the datadir reads back as
// fresh.
func TestPBinRootRecordRealTableIteration(t *testing.T) {
	t.Parallel()

	ms, _ := pbinTestStoredTree(t)
	rootRecord := bytes.Clone(ms.cm[string(pbinRootKey)])
	require.NotEmpty(t, rootRecord)

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx := memdb.BeginRw(t, db)
	for key, record := range ms.cm {
		require.NoError(t, tx.Put(kv.TblCommitmentVals, []byte(key), record))
	}

	cursor, err := tx.Cursor(kv.TblCommitmentVals)
	require.NoError(t, err)
	defer cursor.Close()

	var gotRoot []byte
	seen := 0
	for k, v, err := cursor.First(); k != nil; k, v, err = cursor.Next() {
		require.NoError(t, err)
		require.NotEmpty(t, k, "a zero-length key reads as end-of-stream in domain iteration")
		if bytes.Equal(k, pbinRootKey) {
			gotRoot = bytes.Clone(v)
		}
		seen++
	}
	require.Equal(t, len(ms.cm), seen, "iteration truncated: not every stored record came back")
	require.Equal(t, rootRecord, gotRoot, "root record lost or damaged by the table round-trip")
}

// Every pbinAppendBitPath encoding ends in a trailing bit-count byte ≤ 7, so a
// single byte ≥ 0x08 cannot collide with any encoded path, and pbinDecodeBitPath
// must reject it outright.
func TestPBinRootKeySentinelNotABitPath(t *testing.T) {
	t.Parallel()

	require.Len(t, pbinRootKey, 1)
	require.GreaterOrEqual(t, pbinRootKey[0], byte(0x08))

	_, err := pbinDecodeBitPath(pbinRootKey)
	require.Error(t, err, "a canonical bit-path key must never spell the root key")

	for bitLen := int16(0); bitLen <= pbinMaxPathBits; bitLen++ {
		for _, fill := range []byte{0x00, 0xFF} {
			path := pbinPathFromBits(bytes.Repeat([]byte{fill}, (int(bitLen)+7)/8), bitLen)
			encoded := pbinEncodeBitPath(&path)
			require.NotEqual(t, pbinRootKey, encoded, "bit length %d fill %#x collides with the root key", bitLen, fill)
			require.LessOrEqual(t, encoded[len(encoded)-1], byte(0x07))
		}
	}
}

// loadRoot must tell a fresh datadir from a persisted tree: no record reads back
// as the empty tree, a stored record as the root it was built with.
func TestPBinLoadRootNoRecordVersusStoredTree(t *testing.T) {
	t.Parallel()

	pph, _ := pbinTestEngine(t)
	require.NoError(t, pph.loadRoot())
	require.True(t, pph.rootChecked)
	require.False(t, pph.rootPresent)
	require.Equal(t, pbinNodeEmpty, pph.grid.root.kind)
	root, err := pph.RootHash()
	require.NoError(t, err)
	require.Equal(t, make([]byte, length.Hash), root)

	ms, storedRoot := pbinTestStoredTree(t)
	fresh := NewPBinPatriciaHashed(ms)
	require.NoError(t, fresh.loadRoot())
	require.True(t, fresh.rootPresent)
	require.NotEqual(t, pbinNodeEmpty, fresh.grid.root.kind)
	reloaded, err := fresh.RootHash()
	require.NoError(t, err)
	require.Equal(t, storedRoot, reloaded)
	require.NotEqual(t, make([]byte, length.Hash), reloaded)
}
