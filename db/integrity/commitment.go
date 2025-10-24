package integrity

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func CheckCommitmentKvi(ctx context.Context, db kv.TemporalRoDB, failFast bool, logger log.Logger) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return CheckKvis(tx, kv.CommitmentDomain, failFast, logger)
}

func CheckCommitmentRoot(ctx context.Context, db kv.TemporalRoDB, failFast bool, logger log.Logger) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	aggTx := state.AggTx(tx)
	files := aggTx.Files(kv.CommitmentDomain)
	// atm our older files are missing the root due to purification, so this flag can be used to only check the last file
	onlyCheckLastFile := dbg.EnvBool("CHECK_COMMITMENT_ROOT_ONLY_LAST_FILE", false)
	if onlyCheckLastFile && len(files) > 0 {
		files = files[len(files)-1:]
	}
	var integrityErr error
	for _, file := range files {
		endTxNum := file.EndRootNum()
		logger.Trace("checking commitment root in", "file", file.Fullpath(), "endTxNum", endTxNum)
		v, ok, _, _, err := aggTx.DebugGetLatestFromFiles(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, endTxNum)
		if err != nil {
			return err
		}
		if !ok {
			err = fmt.Errorf("commitment root not found in %s with endTxNum %d", file.Fullpath(), endTxNum)
			if failFast {
				return err
			} else {
				logger.Warn(err.Error())
				integrityErr = AccumulateIntegrityError(integrityErr, err)
				continue
			}
		}
		rootHash, err := commitment.HexTrieExtractStateRoot(v)
		if err != nil {
			err = fmt.Errorf("commitment root in %s with endTxNum %d could not be extracted: %w", file.Fullpath(), endTxNum, err)
			if failFast {
				return err
			} else {
				logger.Warn(err.Error())
				integrityErr = AccumulateIntegrityError(integrityErr, err)
				continue
			}
		}
		if common.BytesToHash(rootHash) == (common.Hash{}) {
			err = fmt.Errorf("commitment root in %s with endTxNum %d is empty", file.Fullpath(), endTxNum)
			if failFast {
				return err
			} else {
				logger.Warn(err.Error())
				integrityErr = AccumulateIntegrityError(integrityErr, err)
			}
		}
	}
	return integrityErr
}
