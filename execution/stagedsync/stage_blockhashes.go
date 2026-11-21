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
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// SpawnBlockHashStage only advances progress: rawdb.WriteHeader indexes each header's hash as it is written.
func SpawnBlockHashStage(s *StageState, tx kv.RwTx) error {
	headNumber, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("getting headers progress: %w", err)
	}
	if s.BlockNumber == headNumber {
		return nil
	}
	return s.Update(tx, headNumber)
}

func UnwindBlockHashStage(u *UnwindState, tx kv.RwTx) error {
	return u.Done(tx)
}
