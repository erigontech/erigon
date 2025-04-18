package stats

import (
	"fmt"
	"runtime"
	"strings"

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
	accFiles := at.DomainFiles(kv.AccountsDomain)
	str := make([]string, 0, len(accFiles))
	for _, item := range accFiles {
		bn, err := tx2block(item.EndRootNum())
		if err != nil {
			logger.Warn("[snapshots:history] Stat", "err", err)
			return
		}
		str = append(str, fmt.Sprintf("%d=%dK", item.EndRootNum()/at.StepSize(), bn/1_000))
	}
	//str2 := make([]string, 0, len(at.storage.files))
	//for _, item := range at.storage.files {
	//	str2 = append(str2, fmt.Sprintf("%s:%dm", item.src.decompressor.FileName(), item.src.decompressor.Count()/1_000_000))
	//}
	//for _, item := range at.commitment.files {
	//	bn := tx2block(item.endTxNum) / 1_000
	//	str2 = append(str2, fmt.Sprintf("%s:%dK", item.src.decompressor.FileName(), bn))
	//}
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
		"txNum2blockNum", strings.Join(str, ","),
		"first_history_idx_in_db", firstHistoryIndexBlockInDB,
		"last_comitment_block", lastCommitmentBlockNum,
		"last_comitment_tx_num", lastCommitmentTxNum,
		//"cnt_in_files", strings.Join(str2, ","),
		//"used_files", strings.Join(at.Files(), ","),
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))

}
