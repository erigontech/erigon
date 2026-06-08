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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
)

type BodiesCfg struct {
	blockReader services.FullBlockReader
	blockWriter *blockio.BlockWriter
}

func StageBodiesCfg(blockReader services.FullBlockReader, blockWriter *blockio.BlockWriter) BodiesCfg {
	return BodiesCfg{blockReader: blockReader, blockWriter: blockWriter}
}

func UnwindBodiesStage(u *UnwindState, tx kv.RwTx, cfg BodiesCfg) error {
	u.UnwindPoint = max(u.UnwindPoint, cfg.blockReader.FrozenBlocks()) // protect from unwind behind files
	err := cfg.blockWriter.MakeBodiesNonCanonical(tx, u.UnwindPoint+1)
	if err != nil {
		return err
	}
	return u.Done(tx)
}
