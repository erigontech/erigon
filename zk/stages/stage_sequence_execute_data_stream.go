package stages

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	verifier "github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"
)

type SequencerBatchStreamWriter struct {
	ctx            context.Context
	logPrefix      string
	legacyVerifier *verifier.LegacyExecutorVerifier
	sdb            *stageDb
	streamServer   *server.DataStreamServer
	hasExecutors   bool
	lastBatch      uint64
}

func newSequencerBatchStreamWriter(batchContext *BatchContext, batchState *BatchState, lastBatch uint64) *SequencerBatchStreamWriter {
	return &SequencerBatchStreamWriter{
		ctx:            batchContext.ctx,
		logPrefix:      batchContext.s.LogPrefix(),
		legacyVerifier: batchContext.cfg.legacyVerifier,
		sdb:            batchContext.sdb,
		streamServer:   batchContext.cfg.datastreamServer,
		hasExecutors:   batchState.hasExecutorForThisBatch,
		lastBatch:      lastBatch,
	}
}

func (sbc *SequencerBatchStreamWriter) CommitNewUpdates() ([]*verifier.VerifierBundle, error) {
	verifierBundles, err := sbc.legacyVerifier.ProcessResultsSequentially()
	if err != nil {
		return nil, err
	}

	return sbc.writeBlockDetailsToDatastream(verifierBundles)
}

func (sbc *SequencerBatchStreamWriter) writeBlockDetailsToDatastream(verifiedBundles []*verifier.VerifierBundle) ([]*verifier.VerifierBundle, error) {
	var checkedVerifierBundles []*verifier.VerifierBundle = make([]*verifier.VerifierBundle, 0, len(verifiedBundles))
	for _, bundle := range verifiedBundles {
		request := bundle.Request
		response := bundle.Response

		if response.Valid {
			parentBlock, err := rawdb.ReadBlockByNumber(sbc.sdb.tx, request.GetLastBlockNumber()-1)
			if err != nil {
				return checkedVerifierBundles, err
			}
			block, err := rawdb.ReadBlockByNumber(sbc.sdb.tx, request.GetLastBlockNumber())
			if err != nil {
				return checkedVerifierBundles, err
			}

			if err := sbc.streamServer.WriteBlockWithBatchStartToStream(sbc.logPrefix, sbc.sdb.tx, sbc.sdb.hermezDb, request.ForkId, request.BatchNumber, sbc.lastBatch, *parentBlock, *block); err != nil {
				return checkedVerifierBundles, err
			}

			// once we have handled the very first block we can update the last batch to be the current batch safely so that
			// we don't keep adding batch bookmarks in between blocks
			sbc.lastBatch = request.BatchNumber
		}

		checkedVerifierBundles = append(checkedVerifierBundles, bundle)

		// just break early if there is an invalid response as we don't want to process the remainder anyway
		if !response.Valid {
			break
		}
	}

	return checkedVerifierBundles, nil
}

func finalizeLastBatchInDatastreamIfNotFinalized(batchContext *BatchContext, batchState *BatchState, thisBlock uint64) error {
	isLastEntryBatchEnd, err := batchContext.cfg.datastreamServer.IsLastEntryBatchEnd()
	if err != nil {
		return err
	}

	if isLastEntryBatchEnd {
		return nil
	}

	log.Warn(fmt.Sprintf("[%s] Last batch %d was not closed properly, closing it now...", batchContext.s.LogPrefix(), batchState.batchNumber))
	ler, err := utils.GetBatchLocalExitRootFromSCStorage(batchState.batchNumber, batchContext.sdb.hermezDb.HermezDbReader, batchContext.sdb.tx)
	if err != nil {
		return err
	}

	lastBlock, err := rawdb.ReadBlockByNumber(batchContext.sdb.tx, thisBlock)
	if err != nil {
		return err
	}
	root := lastBlock.Root()
	if err = batchContext.cfg.datastreamServer.WriteBatchEnd(batchContext.sdb.hermezDb, batchState.batchNumber-1, &root, &ler); err != nil {
		return err
	}
	return nil
}
