package utils

import (
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

// if current sync is before verified batch - short circuit to verified batch, otherwise to enx of next batch
// if there is no new fully downloaded batch - do not short circuit
// returns (shouldShortCircuit, blockNumber, error)
func ShouldShortCircuitExecution(tx kv.RwTx, logPrefix string) (bool, uint64, error) {
	hermezDb := hermez_db.NewHermezDb(tx)

	// get highest verified batch
	highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return false, 0, err
	}

	// get highest executed batch
	executedBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return false, 0, err
	}

	executedBatch, err := hermezDb.GetBatchNoByL2Block(executedBlock)
	if err != nil {
		return false, 0, err
	}

	downloadedBatch, err := hermezDb.GetLatestDownloadedBatchNo()
	if err != nil {
		return false, 0, err
	}

	var shortCircuitBatch uint64
	// if executed lower than verified, short curcuit up to verified
	if executedBatch < highestVerifiedBatchNo {
		shortCircuitBatch = highestVerifiedBatchNo
	} else if executedBatch+1 < downloadedBatch { // else short circuit up to next downloaded batch
		shortCircuitBatch = executedBatch + 1
	} else { // if we don't have at least one more full downlaoded batch, don't short circuit and just execute to latest block
		return false, 0, nil
	}

	// we've got the highest batch to execute to, now get it's highest block
	shortCircuitBlock, err := hermezDb.GetHighestBlockInBatch(shortCircuitBatch)
	if err != nil {
		return false, 0, err
	}

	log.Info(fmt.Sprintf("[%s] Short circuit", logPrefix), "batch", shortCircuitBatch, "block", shortCircuitBlock)

	return true, shortCircuitBlock, nil
}
