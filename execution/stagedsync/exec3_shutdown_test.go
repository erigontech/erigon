package stagedsync

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
)

type blockedExecLoopDB struct {
	kv.TemporalRwDB
	release <-chan struct{}
	err     error
}

func (db *blockedExecLoopDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) {
	<-db.release
	return nil, db.err
}

func TestParallelExecutorShutdownJoinsSchedulerBeforeWorkers(t *testing.T) {
	pe, _ := newResumeTestExec(t, newResumeTestDB(t), chain.AllProtocolChanges)
	synctest.Test(t, func(t *testing.T) {
		releaseExecLoop := make(chan struct{})
		failure := errors.New("exec loop failure")
		pe.cfg.db = &blockedExecLoopDB{release: releaseExecLoop, err: failure}
		_, finish, err := pe.run(t.Context())
		require.NoError(t, err)

		workersStopped := false
		stopWorkers := pe.stopWorkers
		pe.stopWorkers = func() {
			workersStopped = true
			stopWorkers()
		}
		go finish(nil)
		synctest.Wait()
		if workersStopped {
			t.Error("worker contexts closed before the execution loop exited")
		}

		releaseWorker := make(chan struct{})
		pe.runWG.Go(func() { <-releaseWorker })
		close(releaseExecLoop)
		synctest.Wait()
		if workersStopped {
			t.Error("worker contexts closed before a late worker exited")
		}

		close(releaseWorker)
		synctest.Wait()
		require.True(t, workersStopped)
		require.ErrorIs(t, pe.wait(), failure)
	})
}
