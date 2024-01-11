package stages

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zk/types"
)

type IL1Syncer interface {

	// atomic
	IsSyncStarted() bool
	IsDownloading() bool
	GetLastCheckedL1Block() uint64

	// Channels
	GetVerificationsChan() chan types.L1BatchInfo
	GetSequencesChan() chan types.L1BatchInfo
	GetProgressMessageChan() chan string

	Run(lastCheckedBlock uint64)
}

var ErrStateRootMismatch = fmt.Errorf("state root mismatch")

type L1SyncerCfg struct {
	db     kv.RwDB
	syncer IL1Syncer

	zkCfg *ethconfig.Zk
}

func StageL1SyncerCfg(db kv.RwDB, syncer IL1Syncer, zkCfg *ethconfig.Zk) L1SyncerCfg {
	return L1SyncerCfg{
		db:     db,
		syncer: syncer,
		zkCfg:  zkCfg,
	}
}

func SpawnStageL1Syncer(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg L1SyncerCfg,
	firstCycle bool,
	quiet bool,
) error {

	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting L1 sync stage", logPrefix))
	if sequencer.IsSequencer() {
		log.Info(fmt.Sprintf("[%s] skipping -- sequencer", logPrefix))
		return nil
	}
	defer log.Info(fmt.Sprintf("[%s] Finished L1 sync stage ", logPrefix))

	if tx == nil {
		log.Debug("l1 sync: no tx provided, creating a new one")
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("failed to open tx, %w", err)
		}
		defer tx.Rollback()
	}

	// pass tx to the hermezdb
	hermezDb, err := hermez_db.NewHermezDb(tx)
	if err != nil {
		return fmt.Errorf("failed to create hermezdb, %w", err)
	}

	// get l1 block progress from this stage's progress
	l1BlockProgress, err := stages.GetStageProgress(tx, stages.L1Syncer)
	if err != nil {
		return fmt.Errorf("failed to get l1 progress block, %w", err)
	}

	// start syncer if not started
	if !cfg.syncer.IsSyncStarted() {
		if l1BlockProgress == 0 {
			l1BlockProgress = cfg.zkCfg.L1FirstBlock - 1
		}

		// start the syncer
		cfg.syncer.Run(l1BlockProgress)
	}

	verificationsChan := cfg.syncer.GetVerificationsChan()
	sequencesChan := cfg.syncer.GetSequencesChan()
	progressMessageChan := cfg.syncer.GetProgressMessageChan()
	highestVerification := types.L1BatchInfo{}

	newVerificationsCount := 0
	newSequencesCount := 0
Loop:
	for {
		select {
		case verification := <-verificationsChan:
			if verification.BatchNo > highestVerification.BatchNo {
				highestVerification = verification
			}
			if err := hermezDb.WriteVerification(verification.L1BlockNo, verification.BatchNo, verification.L1TxHash, verification.StateRoot); err != nil {
				return fmt.Errorf("failed to write verification for block %d, %w", verification.L1BlockNo, err)
			}
			newVerificationsCount++
		case sequence := <-sequencesChan:
			err = hermezDb.WriteSequence(sequence.L1BlockNo, sequence.BatchNo, sequence.L1TxHash, sequence.StateRoot)
			if err != nil {
				return fmt.Errorf("failed to write batch info, %w", err)
			}
			newSequencesCount++
		case progressMessage := <-progressMessageChan:
			log.Info(fmt.Sprintf("[%s] %s", logPrefix, progressMessage))
		default:
			if !cfg.syncer.IsDownloading() {
				break Loop
			}
		}
	}

	latestCheckedBlock := cfg.syncer.GetLastCheckedL1Block()
	if latestCheckedBlock > l1BlockProgress {
		log.Info(fmt.Sprintf("[%s] Saving L1 syncer progress", logPrefix), "latestCheckedBlock", latestCheckedBlock, "newVerificationsCount", newVerificationsCount, "newSequencesCount", newSequencesCount)

		if err := stages.SaveStageProgress(tx, stages.L1Syncer, latestCheckedBlock); err != nil {
			return fmt.Errorf("failed to save stage progress, %w", err)
		}
		if highestVerification.BatchNo > 0 {
			log.Info(fmt.Sprintf("[%s]", logPrefix), "highestVerificationBatchNo", highestVerification.BatchNo)
			if err := stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, highestVerification.BatchNo); err != nil {
				return fmt.Errorf("failed to save stage progress, %w", err)
			}
		}

		// State Root Verifications Check
		err = verifyAgainstLocalBlocks(tx, hermezDb, logPrefix)
		if err != nil {
			if errors.Is(err, ErrStateRootMismatch) {
				panic(err)
			}
			// do nothing in hope the node will recover if it isn't a stateroot mismatch
		}
	} else {
		log.Info(fmt.Sprintf("[%s] No new L1 blocks to sync", logPrefix))
	}

	if firstCycle {
		log.Debug("l1 sync: first cycle, committing tx")
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit tx, %w", err)
		}
	}

	return nil
}

func UnwindL1SyncerStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg L1SyncerCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	// TODO: implement unwind verifications stage!

	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneL1SyncerStage(s *stagedsync.PruneState, tx kv.RwTx, cfg L1SyncerCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	// TODO: implement prune L1 Verifications stage! (if required)

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func verifyAgainstLocalBlocks(tx kv.RwTx, hermezDb *hermez_db.HermezDb, logPrefix string) (err error) {
	// get the highest hashed block
	hashedBlockNo, err := stages.GetStageProgress(tx, stages.IntermediateHashes)
	if err != nil {
		return fmt.Errorf("failed to get highest hashed block, %w", err)
	}

	// no need to check - interhashes has not yet run
	if hashedBlockNo == 0 {
		return nil
	}

	// get the highest verified block
	verifiedBlockNo, err := hermezDb.GetHighestVerifiedBlockNo()
	if err != nil {
		return fmt.Errorf("failed to get highest verified block no, %w", err)
	}

	// no verifications on l1
	if verifiedBlockNo == 0 {
		return nil
	}

	// 3 scenarios:
	//     1. verified and node both equal
	//     2. node behind l1 - verification block is higher than hashed block - use hashed block to find verification block
	//     3. l1 behind node - verification block is lower than hashed block - use verification block to find hashed block
	blockToCheck := min(verifiedBlockNo, hashedBlockNo)

	// already checked
	highestChecked, err := stages.GetStageProgress(tx, stages.VerificationsStateRootCheck)
	if err != nil {
		return fmt.Errorf("failed to get highest checked block, %w", err)
	}
	if highestChecked >= blockToCheck {
		return nil
	}

	err = blockComparison(tx, hermezDb, blockToCheck, logPrefix)

	if err == nil {
		log.Info(fmt.Sprintf("[%s] State root verified in block %d", logPrefix, blockToCheck))
		if err := stages.SaveStageProgress(tx, stages.VerificationsStateRootCheck, verifiedBlockNo); err != nil {
			return fmt.Errorf("failed to save stage progress, %w", err)
		}
	}

	return err
}

func blockComparison(tx kv.RwTx, hermezDb *hermez_db.HermezDb, blockNo uint64, logPrefix string) error {
	v, err := hermezDb.GetVerificationByL2BlockNo(blockNo)
	if err != nil {
		return fmt.Errorf("failed to get verification by l2 block no, %w", err)
	}

	block, err := rawdb.ReadBlockByNumber(tx, blockNo)
	if err != nil {
		return fmt.Errorf("failed to read block by number, %w", err)
	}

	if v == nil || block == nil {
		log.Info("block or verification is nil", "block", block, "verification", v)
		return nil
	}

	if v.StateRoot != block.Root() {
		log.Error(fmt.Sprintf("[%s] State root mismatch in block %d", logPrefix, blockNo))
		return ErrStateRootMismatch
	}

	return nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
