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

	warnUnevenIIAvailability(at, tx, logger, tx2block)
}

// Standalone inverted indices (logtopics, logaddrs, tracesfrom, tracesto) are
// expected to cover at least the state-history window; an index starting later
// makes filtered eth_getLogs/trace_filter silently incomplete in the gap.
func warnUnevenIIAvailability(at *state.AggregatorRoTx, tx kv.Tx, logger log.Logger, tx2block func(txNum uint64) (uint64, error)) {
	// same definition of "state history starts here" as state.StateHistoryStartTxNum
	historyStartTxNum := min(
		at.HistoryStartFrom(kv.AccountsDomain, tx),
		at.HistoryStartFrom(kv.StorageDomain, tx),
		at.HistoryStartFrom(kv.CodeDomain, tx),
	)
	var late []string
	for id := range at.InvertedIndicesLen() {
		name := at.InvertedIndexName(id)
		iiStartTxNum := at.IIStartFrom(name, tx)
		if iiStartTxNum <= historyStartTxNum {
			continue
		}
		iiFromBlock, err := tx2block(iiStartTxNum)
		if err != nil {
			logger.Debug("[snapshots:history] Stat", "err", err)
			return
		}
		late = append(late, fmt.Sprintf("%s=%d", name.String(), iiFromBlock))
	}
	if len(late) == 0 {
		return
	}
	historyFromBlock, err := tx2block(historyStartTxNum)
	if err != nil {
		logger.Debug("[snapshots:history] Stat", "err", err)
		return
	}
	logger.Debug("[snapshots:history] uneven old-data availability: indices start later than state history; filtered eth_getLogs/trace_filter may return incomplete results for older blocks",
		"state_history_from_block", historyFromBlock,
		"index_from_block", strings.Join(late, ","))
}
