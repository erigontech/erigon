// Copyright 2024 The Erigon Authors
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
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/rlp"
)

type HeadersCfg struct {
	blockReader services.FullBlockReader
}

func StageHeadersCfg(blockReader services.FullBlockReader) HeadersCfg {
	return HeadersCfg{
		blockReader: blockReader,
	}
}

func HeadersUnwind(ctx context.Context, u *UnwindState, s *StageState, tx kv.RwTx, cfg HeadersCfg) (err error) {
	u.UnwindPoint = max(u.UnwindPoint, cfg.blockReader.FrozenBlocks()) // protect from unwind behind files
	// Delete canonical hashes that are being unwound
	unwindBlock := (u.Reason.Block != nil)
	badBlock := false
	if unwindBlock {
		badBlock = u.Reason.IsBadBlock()
	}
	if err := rawdb.TruncateCanonicalHash(tx, u.UnwindPoint+1, badBlock); err != nil {
		return err
	}
	if unwindBlock {
		var maxTd big.Int
		var maxHash common.Hash
		var maxNum uint64 = 0

		// Find header with biggest TD
		tdCursor, cErr := tx.Cursor(kv.HeaderTD)
		if cErr != nil {
			return cErr
		}
		defer tdCursor.Close()
		var k, v []byte
		k, v, err = tdCursor.Last()
		if err != nil {
			return err
		}
		for ; err == nil && k != nil; k, v, err = tdCursor.Prev() {
			if len(k) != 40 {
				return fmt.Errorf("key in TD table has to be 40 bytes long: %x", k)
			}
			var td big.Int
			if err = rlp.DecodeBytes(v, &td); err != nil {
				return err
			}
			if td.Cmp(&maxTd) > 0 {
				maxTd.Set(&td)
				copy(maxHash[:], k[8:])
				maxNum = binary.BigEndian.Uint64(k[:8])
			}
		}
		if err != nil {
			return err
		}
		if maxNum == 0 {
			maxNum = u.UnwindPoint
			var ok bool
			if maxHash, ok, err = cfg.blockReader.CanonicalHash(ctx, tx, maxNum); err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("not found canonical marker: %d", maxNum)
			}
		}
		if err = rawdb.WriteHeadHeaderHash(tx, maxHash); err != nil {
			return err
		}
		if err = u.Done(tx); err != nil {
			return err
		}
		if err = s.Update(tx, maxNum); err != nil {
			return err
		}
	}
	return nil
}
