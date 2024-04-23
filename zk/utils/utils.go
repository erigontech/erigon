package utils

import (
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/chain"
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

	var shortCircuitBatch, shortCircuitBlock, cycle uint64

	// this is so empty batches work
	for shortCircuitBlock == 0 {
		cycle++
		// if executed lower than verified, short curcuit up to verified
		if executedBatch < highestVerifiedBatchNo {
			if downloadedBatch < highestVerifiedBatchNo {
				shortCircuitBatch = downloadedBatch
			} else {
				shortCircuitBatch = highestVerifiedBatchNo
			}
		} else if executedBatch+cycle <= downloadedBatch { // else short circuit up to next downloaded batch
			shortCircuitBatch = executedBatch + cycle
		} else { // if we don't have at least one more full downlaoded batch, don't short circuit and just execute to latest block
			return false, 0, nil
		}

		// we've got the highest batch to execute to, now get it's highest block
		shortCircuitBlock, err = hermezDb.GetHighestBlockInBatch(shortCircuitBatch)
		if err != nil {
			return false, 0, err
		}
	}

	log.Info(fmt.Sprintf("[%s] Short circuit", logPrefix), "batch", shortCircuitBatch, "block", shortCircuitBlock)

	return true, shortCircuitBlock, nil
}

type ForkReader interface {
	GetForkIdBlock(forkId uint64) (uint64, error)
}

func UpdateZkEVMBlockCfg(cfg *chain.Config, hermezDb ForkReader, logPrefix string) error {
	var higherForkIdBlockNum uint64
	for _, forkId := range chain.ForkIdsOrdered {
		blockNum, err := hermezDb.GetForkIdBlock(uint64(forkId))
		if err != nil {
			log.Error(fmt.Sprintf("[%s] Error getting fork id %v from db: %v", logPrefix, forkId, err))
			return err
		}
		if blockNum == 0 && higherForkIdBlockNum != 0 {
			blockNum = higherForkIdBlockNum
		}

		higherForkIdBlockNum = blockNum

		if err := cfg.SetForkIdBlock(forkId, blockNum); err != nil {
			log.Error(fmt.Sprintf("[%s] Error setting fork id %v to block %v", logPrefix, forkId, blockNum))
			return err
		}
	}

	return nil
}
