package stagedsync

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/params"
)

func apply(tx kv.RwTx, logger log.Logger) (beforeBlock, afterBlock testGenHook, w state.StateWriter) {
	domains, err := libstate.NewSharedDomains(tx, logger)
	if err != nil {
		panic(err)
	}
	rs := state.NewStateV3(domains, logger)
	stateWriter := state.NewStateWriterBufferedV3(rs)
	stateWriter.SetTx(tx)

	return func(n, from, numberOfBlocks uint64) {
			stateWriter.SetTxNum(context.Background(), n)
			stateWriter.ResetWriteSet()
		}, func(n, from, numberOfBlocks uint64) {
			txTask := &state.TxTask{
				BlockNum:   n,
				Rules:      params.TestRules,
				TxNum:      n,
				TxIndex:    0,
				Final:      true,
				WriteLists: stateWriter.WriteSet(),
			}
			txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = stateWriter.PrevAndDels()
			rs.SetTxNum(txTask.TxNum, txTask.BlockNum)
			if err := rs.ApplyState4(context.Background(), txTask); err != nil {
				panic(err)
			}
			_, err := rs.Domains().ComputeCommitment(context.Background(), true, txTask.BlockNum, "")
			if err != nil {
				panic(err)
			}

			if n == from+numberOfBlocks-1 {
				if err := domains.Flush(context.Background(), tx); err != nil {
					panic(err)
				}
			}
		}, stateWriter
}

func newAgg(t *testing.T, logger log.Logger) *libstate.Aggregator {
	t.Helper()
	dirs, ctx := datadir.New(t.TempDir()), context.Background()
	agg, err := libstate.NewAggregator(ctx, dirs, config3.HistoryV3AggregationStep, nil, logger)
	require.NoError(t, err)
	err = agg.OpenFolder(false)
	require.NoError(t, err)
	return agg
}
