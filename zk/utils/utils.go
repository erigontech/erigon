package utils

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func ShouldShortCircuitExecution(tx kv.RwTx) (bool, uint64, error) {
	intersProgress, err := stages.GetStageProgress(tx, stages.IntermediateHashes)
	if err != nil {
		return false, 0, err
	}

	hermezDb, err := hermez_db.NewHermezDb(tx)
	if err != nil {
		return false, 0, err
	}

	// if there is no inters progress - i.e. first sync, don't skip exec, and execute to the highest block in the highest verified batch that we did download
	if intersProgress == 0 {
		highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
		if err != nil {
			return false, 0, err
		}

		// checking which is the highest batch we downloaded, in some cases
		// (e.g. on a bad network), we might have not downloaded the highest verified batch yet...
		highestDownloadedBatchNo, err := hermezDb.GetLatestDownloadedBatchNo()
		if err != nil {
			return false, 0, err
		}

		batchToCheck := highestVerifiedBatchNo
		// in that case, we want to check up to the last batch we downloaded
		// (otherwie we have no blocks to return)
		if highestDownloadedBatchNo < highestVerifiedBatchNo {
			batchToCheck = highestDownloadedBatchNo
		}

		// we could find ourselves with a batch with no blocks here, so we want to go back one batch at
		// a time until we find a batch with blocks
		max := uint64(0)
		killSwitch := 0
		for {
			max, err = hermezDb.GetHighestBlockInBatch(batchToCheck)
			if err != nil {
				return false, 0, err
			}
			if max != 0 {
				break
			}
			batchToCheck--
			killSwitch++
			if killSwitch > 100 {
				return false, 0, fmt.Errorf("could not find a batch with blocks when checking short circuit")
			}
		}

		return false, max, nil
	}

	highestHashableL2BlockNo, err := stages.GetStageProgress(tx, stages.HighestHashableL2BlockNo)
	if err != nil {
		return false, 0, err
	}
	highestHashableBatchNo, err := hermezDb.GetBatchNoByL2Block(highestHashableL2BlockNo)
	if err != nil {
		return false, 0, err
	}
	intersProgressBatchNo, err := hermezDb.GetBatchNoByL2Block(intersProgress)
	if err != nil {
		return false, 0, err
	}

	// check to skip execution: 1. there is inters progress, 2. the inters progress is less than the highest hashable, 3. we're in the tip batch range
	if intersProgress != 0 && intersProgress < highestHashableL2BlockNo && highestHashableBatchNo-intersProgressBatchNo <= 1 {
		return true, highestHashableL2BlockNo, nil
	}

	return false, 0, nil
}

func ShouldIncrementInterHashes(tx kv.RwTx) (bool, uint64, error) {
	return ShouldShortCircuitExecution(tx)
}
