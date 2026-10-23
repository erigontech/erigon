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

package state

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type historyGateDebugTx struct {
	kv.TemporalDebugTx
	coverageStart uint64
	filesEnd      uint64
	recorded      bool
	lookupErr     error
}

func (d historyGateDebugTx) HistoryCoverageStart(kv.Domain) uint64 { return d.coverageStart }

func (d historyGateDebugTx) TxNumsInFiles(...kv.Domain) uint64 { return d.filesEnd }

func (d historyGateDebugTx) HistoryRecordedInDB(kv.Domain, uint64, uint64) (bool, error) {
	return d.recorded, d.lookupErr
}

// TestRequireStateHistory pins when state history may serve reads at a past
// txNum: history files prove their coverage, while history held only in the DB
// must show a record in the range, because an empty history table reports no
// start and a history read then answers from the latest state.
func TestRequireStateHistory(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		dbg  historyGateDebugTx
		ok   bool
	}{
		{"history starts after the range", historyGateDebugTx{coverageStart: 10, filesEnd: 20, recorded: true}, false},
		{"history files cover the range", historyGateDebugTx{filesEnd: 20}, true},
		{"DB history holds a record", historyGateDebugTx{recorded: true}, true},
		{"DB history holds no record", historyGateDebugTx{}, false},
		{"DB history lookup fails", historyGateDebugTx{recorded: true, lookupErr: errors.New("history read failed")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := RequireStateHistory(tc.dbg, 5, 6)
			if tc.ok {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tc.dbg.lookupErr == nil {
				require.ErrorIs(t, err, ErrPruned)
			}
		})
	}
}
