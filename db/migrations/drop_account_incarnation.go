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
	"context"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// dropAccountIncarnation rewrites every AccountsDomain row from the legacy
// 4-section SerialiseV3 layout
//
//	[nonceLen|nonce][balLen|bal][codeLen|codeHash][incLen|inc]
//
// to the 3-section layout
//
//	[nonceLen|nonce][balLen|bal][codeLen|codeHash]
//
// after accounts.Account.Incarnation was removed (Erigon issue #12440).
//
// # Why a migration is needed
//
// DeserialiseV3 is tolerant — it silently skips any trailing bytes — so
// stale rows decode correctly without this migration. Re-encoding in place
// reclaims ~1 byte per row (more for the rare rows whose Incarnation was
// non-zero before the cleanup) and lets follow-up cleanups drop the
// tolerant branch on schedule.
//
// # Scope
//
// Touches the live MDBX state only: kv.TblAccountVals (current) and
// kv.TblAccountHistoryVals (history). AccountsDomain snapshot files
// (frozen .kv segments) are immutable and stay in the legacy format
// until they are rebuilt by the regular snapshot pipeline; the tolerant
// decoder handles them transparently in the meantime.
//
// The migration is safe on already-migrated databases — re-encoding a
// row already in the 3-section format is a no-op.
var dropAccountIncarnation = Migration{
	Name: "drop_account_incarnation",
	Up: func(db kv.RwDB, dirs datadir.Dirs, _ []byte, BeforeCommit Callback, logger log.Logger) error {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for _, table := range []string{kv.TblAccountVals, kv.TblAccountHistoryVals} {
			if err := reencodeAccountTable(tx, table, dirs, logger); err != nil {
				return fmt.Errorf("re-encode %s: %w", table, err)
			}
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}

// reencodeAccountTable walks every DupSort entry in a SerialiseV3-encoded
// AccountsDomain table, decodes the `[step(8)][value]` pair, re-encodes
// the value in the 3-section format, then ClearTable+Load via ETL.
func reencodeAccountTable(tx kv.RwTx, table string, dirs datadir.Dirs, logger log.Logger) error {
	collector := etl.NewCollector("[migration] drop_account_incarnation/"+table,
		dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer collector.Close()

	c, err := tx.RwCursorDupSort(table)
	if err != nil {
		return err
	}
	defer c.Close()

	var acc accounts.Account
	var seen, rewritten int
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		seen++

		// Layout: first 8 bytes are the encoded step number; remainder is
		// the SerialiseV3-encoded account body (or empty for history rows
		// representing "did not exist").
		if len(v) < 8 {
			return fmt.Errorf("malformed value at key %x: %d bytes (need >= 8 for step prefix)", k, len(v))
		}
		body := v[8:]
		if len(body) == 0 {
			// Empty body — pass through unchanged.
			if err := collector.Collect(k, v); err != nil {
				return err
			}
			continue
		}

		if err := accounts.DeserialiseV3(&acc, body); err != nil {
			return fmt.Errorf("DeserialiseV3 at key %x: %w", k, err)
		}
		newBody := accounts.SerialiseV3(&acc)

		newV := make([]byte, 8+len(newBody))
		copy(newV[:8], v[:8])
		copy(newV[8:], newBody)

		if len(newV) != len(v) {
			rewritten++
		}
		if err := collector.Collect(k, newV); err != nil {
			return err
		}
	}
	c.Close()

	if err := tx.ClearTable(table); err != nil {
		return err
	}
	if err := collector.Load(tx, table, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	logger.Info("[migration] drop_account_incarnation: re-encoded table",
		"table", table, "rows", seen, "rewritten", rewritten)
	return nil
}
