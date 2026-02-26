package stats

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
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
		if !strings.HasSuffix(item.Fullpath(), ".kv") {
			continue
		}
		bn, err := tx2block(item.EndRootNum())
		if err != nil {
			logger.Warn("[snapshots:history] Stat", "err", err)
			return
		}
		str = append(str, fmt.Sprintf("%d=%s", item.EndRootNum()/at.StepSize(), common.PrettyCounter(bn)))
	}

	firstHistoryIndexBlockInDB, err := tx2block(at.MinStepInDb(tx, kv.AccountsDomain) * at.StepSize())
	if err != nil {
		logger.Warn("[snapshots:history] Stat", "err", err)
		return
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info("[snapshots:history] Stat",
		"blocks", common.PrettyCounter(domainBlockNumProgress+1),
		"step2blockNum", strings.Join(str, ","),
		"txs", common.PrettyCounter(at.Agg().EndTxNumMinimax()),
		"first_history_idx_in_db", firstHistoryIndexBlockInDB,
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
}
