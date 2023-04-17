package stages

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/sync_stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/types"
)

type IVerificationsSyncer interface {
	GetVerifications(logPrefix string, startBlock uint64) (verifications []types.L1BatchInfo, highestL1Block uint64, err error)
}

var ErrStateRootMismatch = fmt.Errorf("state root mismatch")

type L1VerificationsCfg struct {
	db     kv.RwDB
	syncer IVerificationsSyncer

	zkCfg *ethconfig.Zk
}

func StageL1VerificationsCfg(db kv.RwDB, syncer IVerificationsSyncer, zkCfg *ethconfig.Zk) L1VerificationsCfg {
	return L1VerificationsCfg{
		db:     db,
		syncer: syncer,
		zkCfg:  zkCfg,
	}
}

func SpawnStageL1Verifications(
	s *sync_stages.StageState,
	u sync_stages.Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg L1VerificationsCfg,
	firstCycle bool,
	quiet bool,
) error {

	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting L1 Verifications download stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished L1 Verifications download stage ", logPrefix))

	if tx == nil {
		log.Debug("l1 verifications: no tx provided, creating a new one")
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
	l1BlockProgress, err := sync_stages.GetStageProgress(tx, sync_stages.L1Verifications)
	if err != nil {
		return fmt.Errorf("failed to get l1 progress block, %w", err)
	}

	if l1BlockProgress == 0 {
		l1BlockProgress = cfg.zkCfg.L1FirstBlock - 1
	}

	// get as many verifications as we can from the network
	log.Info(fmt.Sprintf("[%s] Downloading verifications from L1...", logPrefix))

	verifications, _, err := cfg.syncer.GetVerifications(logPrefix, l1BlockProgress+1)
	if err != nil {
		return fmt.Errorf("failed to get l1 verifications from network, %w", err)
	}

	if len(verifications) > 0 {
		highestVerification := types.L1BatchInfo{}
		log.Info(fmt.Sprintf("[%s] L1 verifications downloaded", logPrefix), "count", len(verifications))
		log.Info(fmt.Sprintf("[%s] Saving verifications...", logPrefix))

		// write verifications to the hermezdb
		for _, verification := range verifications {
			if verification.BatchNo > highestVerification.BatchNo {
				highestVerification = verification
			}
			if err := hermezDb.WriteVerification(verification.L1BlockNo, verification.BatchNo, verification.L1TxHash, verification.StateRoot); err != nil {
				return fmt.Errorf("failed to write verification for block %d, %w", verification.L1BlockNo, err)
			}
		}
		log.Info(fmt.Sprintf("[%s] Finished saving verifications to DB.", logPrefix))

		// update stage progress - highest l1 block containing a verification
		if err := sync_stages.SaveStageProgress(tx, sync_stages.L1Verifications, highestVerification.L1BlockNo); err != nil {
			return fmt.Errorf("failed to save stage progress, %w", err)
		}
		if err := sync_stages.SaveStageProgress(tx, sync_stages.L1VerificationsBatchNo, highestVerification.BatchNo); err != nil {
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

	if firstCycle {
		log.Debug("l1 verifications: first cycle, committing tx")
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit tx, %w", err)
		}
	}

	return nil
}

func UnwindL1VerificationsStage(u *sync_stages.UnwindState, tx kv.RwTx, cfg L1VerificationsCfg, ctx context.Context) (err error) {
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

func PruneL1VerificationsStage(s *sync_stages.PruneState, tx kv.RwTx, cfg L1VerificationsCfg, ctx context.Context) (err error) {
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
	hashedBlockNo, err := sync_stages.GetStageProgress(tx, sync_stages.IntermediateHashes)
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
	highestChecked, err := sync_stages.GetStageProgress(tx, sync_stages.VerificationsStateRootCheck)
	if err != nil {
		return fmt.Errorf("failed to get highest checked block, %w", err)
	}
	if highestChecked >= blockToCheck {
		return nil
	}

	err = blockComparison(tx, hermezDb, blockToCheck, logPrefix)

	if err == nil {
		log.Info(fmt.Sprintf("[%s] State root verified in block %d", logPrefix, blockToCheck))
		if err := sync_stages.SaveStageProgress(tx, sync_stages.VerificationsStateRootCheck, verifiedBlockNo); err != nil {
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
