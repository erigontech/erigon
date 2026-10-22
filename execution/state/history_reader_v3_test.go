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
