package utils

import (
	"fmt"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/constants"
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
	GetLowestBatchByFork(forkId uint64) (uint64, error)
	GetLowestBlockInBatch(batchNo uint64) (blockNo uint64, found bool, err error)
}

type ForkConfigWriter interface {
	SetForkIdBlock(forkId constants.ForkId, blockNum uint64) error
}

type DbReader interface {
	GetLocalExitRootForBatchNo(batchNo uint64) (libcommon.Hash, error)
	GetHighestBlockInBatch(batchNo uint64) (uint64, error)
}

func UpdateZkEVMBlockCfg(cfg ForkConfigWriter, hermezDb ForkReader, logPrefix string) error {
	var lastSetBlockNum uint64 = 0
	var foundAny bool = false

	for _, forkId := range chain.ForkIdsOrdered {
		batch, err := hermezDb.GetLowestBatchByFork(uint64(forkId))
		if err != nil {
			return err
		}
		blockNum, found, err := hermezDb.GetLowestBlockInBatch(batch)
		if err != nil {
			return err
		}

		if found {
			lastSetBlockNum = blockNum
			foundAny = true
		} else if !foundAny {
			log.Trace(fmt.Sprintf("[%s] No block number found for fork id %v and no previous block number set", logPrefix, forkId))
			continue
		} else {
			log.Trace(fmt.Sprintf("[%s] No block number found for fork id %v, using last set block number: %v", logPrefix, forkId, lastSetBlockNum))
		}

		if err := cfg.SetForkIdBlock(forkId, lastSetBlockNum); err != nil {
			log.Error(fmt.Sprintf("[%s] Error setting fork id %v to block %v", logPrefix, forkId, lastSetBlockNum))
			return err
		}
	}

	return nil
}

func RecoverySetBlockConfigForks(blockNum uint64, forkId uint64, cfg ForkConfigWriter, logPrefix string) error {
	for _, fork := range chain.ForkIdsOrdered {
		if uint64(fork) <= forkId {
			if err := cfg.SetForkIdBlock(fork, blockNum); err != nil {
				log.Error(fmt.Sprintf("[%s] Error setting fork id %v to block %v", logPrefix, forkId, blockNum))
				return err
			}
		}
	}

	return nil
}

func GetBatchLocalExitRoot(batchNo uint64, db DbReader, tx kv.Tx) (libcommon.Hash, error) {
	// check db first
	localExitRoot, err := db.GetLocalExitRootForBatchNo(batchNo)
	if err != nil {
		return libcommon.Hash{}, err
	}

	if localExitRoot != (libcommon.Hash{}) {
		return localExitRoot, nil
	}

	return GetBatchLocalExitRootFromSCStorage(batchNo, db, tx)
}

func GetBatchLocalExitRootFromSCStorage(batchNo uint64, db DbReader, tx kv.Tx) (libcommon.Hash, error) {
	var localExitRoot libcommon.Hash

	if batchNo > 0 {
		checkBatch := batchNo

		stateReader := state.NewPlainStateReadAccountStorage(tx, 0)
		defer stateReader.Close()

		for ; checkBatch > 0; checkBatch-- {
			blockNo, err := db.GetHighestBlockInBatch(checkBatch)
			if err != nil {
				return libcommon.Hash{}, err
			}

			// stateReader := state.NewPlainStateReadAccountStorage(tx, blockNo)
			// defer stateReader.Close()
			stateReader.SetBlockNr(blockNo)
			rawLer, err := stateReader.ReadAccountStorage(state.GER_MANAGER_ADDRESS, 1, &state.GLOBAL_EXIT_ROOT_POS_1)
			if err != nil {
				return libcommon.Hash{}, err
			}
			localExitRoot = libcommon.BytesToHash(rawLer)
			if localExitRoot != (libcommon.Hash{}) {
				break
			}
		}
	}

	return localExitRoot, nil
}
