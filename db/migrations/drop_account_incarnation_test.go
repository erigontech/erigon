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

package migrations

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// stepPrefix returns a deterministic 8-byte step prefix for fixture rows.
func stepPrefix(b byte) []byte { return []byte{b, 0, 0, 0, 0, 0, 0, 0} }

// legacy4Section builds a row body in the pre-moksha 4-section SerialiseV3
// format: [nonceLen|nonce][balLen|bal][codeLen|codeHash][incLen|inc]. Used to
// seed fixture rows that DeserialiseV3 must still decode tolerantly.
func legacy4Section(nonce uint64, balance uint64, codeHash *common.Hash, incarnation uint64) []byte {
	var out []byte

	if nonce == 0 {
		out = append(out, 0)
	} else {
		nb := []byte{byte(nonce)}
		for v := nonce >> 8; v != 0; v >>= 8 {
			nb = append(nb, byte(v))
		}
		// Bytes were appended little-endian; flip to big-endian.
		for i, j := 0, len(nb)-1; i < j; i, j = i+1, j-1 {
			nb[i], nb[j] = nb[j], nb[i]
		}
		out = append(out, byte(len(nb)))
		out = append(out, nb...)
	}

	if balance == 0 {
		out = append(out, 0)
	} else {
		bb := []byte{byte(balance)}
		for v := balance >> 8; v != 0; v >>= 8 {
			bb = append(bb, byte(v))
		}
		for i, j := 0, len(bb)-1; i < j; i, j = i+1, j-1 {
			bb[i], bb[j] = bb[j], bb[i]
		}
		out = append(out, byte(len(bb)))
		out = append(out, bb...)
	}

	if codeHash == nil {
		out = append(out, 0)
	} else {
		out = append(out, 32)
		out = append(out, codeHash[:]...)
	}

	if incarnation == 0 {
		out = append(out, 0)
	} else {
		ib := []byte{byte(incarnation)}
		for v := incarnation >> 8; v != 0; v >>= 8 {
			ib = append(ib, byte(v))
		}
		for i, j := 0, len(ib)-1; i < j; i, j = i+1, j-1 {
			ib[i], ib[j] = ib[j], ib[i]
		}
		out = append(out, byte(len(ib)))
		out = append(out, ib...)
	}
	return out
}

// TestDropAccountIncarnation_ReencodeAccountTable seeds an AccountsDomain
// table with a mix of legacy-4-section rows, already-migrated 3-section
// rows, history tombstones (empty body) and rows with the step prefix only,
// then asserts:
//
//   - every row decodes to the same Account post-migration
//   - the 8-byte step prefix survives intact
//   - the migration is idempotent (re-running yields byte-identical output)
//   - legacy rows shrink to the 3-section length (loses the trailing
//     [incLen|inc] pair)
func TestDropAccountIncarnation_ReencodeAccountTable(t *testing.T) {
	t.Parallel()
	codeHash := common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3}))

	type row struct {
		key  []byte
		body []byte
		acc  accounts.Account
	}

	rows := []row{
		// Legacy 4-section row with incarnation=5.
		{
			key:  []byte("legacy-with-inc"),
			body: legacy4Section(7, 1000, &codeHash, 5),
			acc: accounts.Account{
				Nonce:    7,
				Balance:  *uint256.NewInt(1000),
				CodeHash: accounts.InternCodeHash(codeHash),
			},
		},
		// Legacy 4-section row with incarnation=0 (still legacy — explicit
		// trailing [0] byte for the empty incarnation section).
		{
			key:  []byte("legacy-inc-zero"),
			body: legacy4Section(2, 500, &codeHash, 0),
			acc: accounts.Account{
				Nonce:    2,
				Balance:  *uint256.NewInt(500),
				CodeHash: accounts.InternCodeHash(codeHash),
			},
		},
		// EOA: no codeHash, no incarnation (legacy).
		{
			key:  []byte("legacy-eoa"),
			body: legacy4Section(1, 42, nil, 0),
			acc: accounts.Account{
				Nonce:    1,
				Balance:  *uint256.NewInt(42),
				CodeHash: accounts.EmptyCodeHash,
			},
		},
		// Already-migrated 3-section row — re-encoding must be a no-op.
		{
			key:  []byte("modern-3-section"),
			body: accounts.SerialiseV3(&accounts.Account{Nonce: 9, Balance: *uint256.NewInt(7), CodeHash: accounts.InternCodeHash(codeHash)}),
			acc: accounts.Account{
				Nonce:    9,
				Balance:  *uint256.NewInt(7),
				CodeHash: accounts.InternCodeHash(codeHash),
			},
		},
	}

	// History tombstone: step prefix followed by zero-length body. The
	// migration must pass it through untouched.
	tombstoneKey := []byte("history-tombstone")
	tombstoneVal := stepPrefix(0x42) // step prefix only, no body

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	for i, r := range rows {
		v := append(stepPrefix(byte(i+1)), r.body...)
		require.NoError(t, tx.Put(kv.TblAccountVals, r.key, v))
	}
	require.NoError(t, tx.Put(kv.TblAccountVals, tombstoneKey, tombstoneVal))

	require.NoError(t, reencodeAccountTable(tx, kv.TblAccountVals, datadir.Dirs{Tmp: t.TempDir()}, log.New()))

	// Verify each row decodes to the same Account, the step prefix is
	// intact, and legacy rows have shrunk to the 3-section length.
	threeSectionLen := func(a *accounts.Account) int { return len(accounts.SerialiseV3(a)) }

	for i, r := range rows {
		stored, err := tx.GetOne(kv.TblAccountVals, r.key)
		require.NoError(t, err)
		require.NotEmpty(t, stored, "row %q missing after migration", r.key)
		require.Equal(t, byte(i+1), stored[0], "row %q lost its step prefix", r.key)

		var got accounts.Account
		require.NoError(t, accounts.DeserialiseV3(&got, stored[8:]), "row %q failed to decode", r.key)
		require.Equal(t, r.acc.Nonce, got.Nonce, "row %q nonce mismatch", r.key)
		require.Equal(t, r.acc.Balance.Uint64(), got.Balance.Uint64(), "row %q balance mismatch", r.key)
		require.Equal(t, r.acc.CodeHash, got.CodeHash, "row %q codeHash mismatch", r.key)

		require.Equal(t, 8+threeSectionLen(&r.acc), len(stored),
			"row %q should be exactly 8 (step) + 3-section length after migration", r.key)
	}

	// Tombstone is passed through unchanged.
	storedTomb, err := tx.GetOne(kv.TblAccountVals, tombstoneKey)
	require.NoError(t, err)
	require.Equal(t, tombstoneVal, storedTomb, "history tombstone must be preserved verbatim")

	// Idempotence: snapshot the migrated table, re-run the migration, and
	// confirm the table is byte-identical.
	snapshot := map[string][]byte{}
	cur, err := tx.RwCursorDupSort(kv.TblAccountVals)
	require.NoError(t, err)
	defer cur.Close()
	for k, v, err := cur.First(); k != nil; k, v, err = cur.Next() {
		require.NoError(t, err)
		snapshot[string(k)] = append([]byte(nil), v...)
	}
	cur.Close()

	require.NoError(t, reencodeAccountTable(tx, kv.TblAccountVals, datadir.Dirs{Tmp: t.TempDir()}, log.New()))

	for k, want := range snapshot {
		got, err := tx.GetOne(kv.TblAccountVals, []byte(k))
		require.NoError(t, err)
		require.Equal(t, want, got, "idempotence violation at key %q", k)
	}
}
