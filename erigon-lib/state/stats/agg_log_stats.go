package stats

import (
	"runtime"

	common2 "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
)

func LogStats(at *state.AggregatorRoTx, tx kv.Tx, logger log.Logger, tx2block func(endTxNumMinimax uint64) (uint64, error)) {
	maxTxNum := at.TxNumsInFiles(kv.StateDomains...)
	if maxTxNum == 0 {
		return
	}

	domainBlockNumProgress, err := tx2block(maxTxNum)
	if err != nil {
		logger.Warn("[snapshots:history] Stat", "err", err)
		return
	}

	var lastCommitmentBlockNum, lastCommitmentTxNum uint64
	commFiles := at.DomainFiles(kv.CommitmentDomain)
	if len(commFiles) > 0 {
		lastCommitmentTxNum = commFiles[len(commFiles)-1].EndRootNum()
		lastCommitmentBlockNum, err = tx2block(lastCommitmentTxNum)
		if err != nil {
			logger.Warn("[snapshots:history] Stat", "err", err)
			return
		}
	}
	firstHistoryIndexBlockInDB, err := tx2block(at.MinStepInDb(tx, kv.AccountsDomain) * at.StepSize())
	if err != nil {
		logger.Warn("[snapshots:history] Stat", "err", err)
		return
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info("[snapshots:history] Stat",
		"blocks", common2.PrettyCounter(domainBlockNumProgress+1),
		"txs", common2.PrettyCounter(at.Agg().EndTxNumMinimax()),
		"first_history_idx_in_db", firstHistoryIndexBlockInDB,
		"last_commitment_block", lastCommitmentBlockNum,
		"last_commitment_tx_num", lastCommitmentTxNum,
		//"cnt_in_files", strings.Join(str2, ","),
		//"used_files", strings.Join(at.Files(), ","),
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))

}
