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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
)

var removeOrphanedBlockBodies = Migration{
	Name: "remove_orphaned_block_bodies",
	Up: func(db kv.RwDB, _ datadir.Dirs, _ []byte, beforeCommit Callback, logger log.Logger) error {
		ctx := context.Background()
		var next []byte
		var removed uint64
		for {
			if err := db.Update(ctx, func(tx kv.RwTx) error {
				cursor, cursorErr := tx.Cursor(kv.BlockBody)
				if cursorErr != nil {
					return cursorErr
				}
				defer cursor.Close()
				key, _, cursorErr := cursor.Seek(next)
				for scanned := 0; key != nil && scanned < 1000; scanned++ {
					if cursorErr != nil {
						return cursorErr
					}
					if len(key) != dbutils.NumberLength+length.Hash {
						return fmt.Errorf("invalid block body key length: %d", len(key))
					}
					number := binary.BigEndian.Uint64(key)
					hash := common.BytesToHash(key[dbutils.NumberLength:])
					hasHeader, err := tx.Has(kv.Headers, key)
					if err != nil {
						return err
					}
					if number != 0 && !hasHeader {
						indexed, err := tx.Has(kv.HeaderNumber, hash[:])
						if err != nil {
							return err
						}
						canonical, err := rawdb.ReadCanonicalHash(tx, number)
						if err != nil {
							return err
						}
						if !indexed && canonical != hash {
							if err := rawdb.DeleteBlock(tx, hash, number); err != nil {
								return err
							}
							removed++
						}
					}
					key, _, cursorErr = cursor.Next()
				}
				if cursorErr != nil {
					return cursorErr
				}
				next = bytes.Clone(key)
				if next == nil {
					return beforeCommit(tx, nil, true)
				}
				return nil
			}); err != nil {
				return err
			}
			if next == nil {
				break
			}
		}
		logger.Info("Removed orphaned block bodies", "blocks", removed)
		return nil
	},
}
