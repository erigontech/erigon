// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commands

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/debug"
)

var (
	historyAccountDebugAddress string
	historyAccountDebugTxIndex int
)

func init() {
	withDataDir(historyAccountDebugCmd)
	withChain(historyAccountDebugCmd)
	withBlock(historyAccountDebugCmd)
	historyAccountDebugCmd.Flags().StringVar(&historyAccountDebugAddress, "address", "", "EOA/contract address to inspect")
	historyAccountDebugCmd.Flags().IntVar(&historyAccountDebugTxIndex, "tx-index", 0, "Transaction index within the block")
	must(historyAccountDebugCmd.MarkFlagRequired("address"))
	rootCmd.AddCommand(historyAccountDebugCmd)
}

var historyAccountDebugCmd = &cobra.Command{
	Use:     "debug_historical_account",
	Short:   "Inspect one account across historical txNum boundaries for a block",
	Example: "./build/bin/integration debug_historical_account --datadir=/erigon-data/mainnet_archive --chain mainnet --block 204336 --tx-index 4 --address 0x49eb9Ab0F00e6Db84B36e8dD9a1Bd44eb6cbd603",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		addr, err := parseAddress(historyAccountDebugAddress)
		if err != nil {
			logger.Error("invalid address", "address", historyAccountDebugAddress, "err", err)
			return
		}

		dirs := datadir.New(datadirCli)
		chainDB, err := openDB(dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDB.Close()

		rawDBProvider, ok := chainDB.(interface{ InternalDB() kv.RwDB })
		if !ok {
			logger.Error("temporal db does not expose underlying raw db", "type", fmt.Sprintf("%T", chainDB))
			return
		}
		rawDB := rawDBProvider.InternalDB()
		blockSnapshots, borSnapshots, _, _, _, _, err := allSnapshots(ctx, rawDB, logger)
		if err != nil {
			logger.Error("open snapshots", "error", err)
			return
		}
		blockReader := freezeblocks.NewBlockReader(blockSnapshots, borSnapshots)

		if err := chainDB.View(ctx, func(tx kv.Tx) error {
			ttx, ok := tx.(kv.TemporalTx)
			if !ok {
				return fmt.Errorf("tx %T does not implement kv.TemporalTx", tx)
			}
			return inspectHistoricalAccount(ctx, ttx, blockReader, blockReader.TxnumReader(), block, historyAccountDebugTxIndex, addr, logger)
		}); err != nil {
			logger.Error("inspect historical account", "err", err)
		}
	},
}

func inspectHistoricalAccount(ctx context.Context, tx kv.TemporalTx, blockReader *freezeblocks.BlockReader, txNumsReader rawdbv3.TxNumsReader, blockNum uint64, txIndex int, addr accounts.Address, logger log.Logger) error {
	minTxNum, err := txNumsReader.Min(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	var maxPrevTxNum uint64
	if blockNum > 0 {
		maxPrevTxNum, err = txNumsReader.Max(ctx, tx, blockNum-1)
		if err != nil {
			return err
		}
	}

	firstSystemTxNum := minTxNum
	firstUserTxNum := minTxNum + 1
	currentTxNum := firstUserTxNum + uint64(txIndex)
	historyStartFrom := state.StateHistoryStartTxNum(tx)

	logBodySnapshotBoundaries(ctx, tx, blockReader, blockNum, logger)
	logger.Info("[historical_account] boundaries",
		"block", blockNum,
		"txIndex", txIndex,
		"address", addr,
		"historyStartFrom", historyStartFrom,
		"maxPrevTxNum", maxPrevTxNum,
		"firstSystemTxNum", firstSystemTxNum,
		"firstUserTxNum", firstUserTxNum,
		"currentTxNum", currentTxNum,
	)

	logHistoricalAccountSample(tx, addr, "prevBlockEnd", maxPrevTxNum, logger)
	logHistoricalAccountSample(tx, addr, "firstSystem", firstSystemTxNum, logger)
	logHistoricalAccountSample(tx, addr, "firstUser", firstUserTxNum, logger)
	logHistoricalAccountSample(tx, addr, "current", currentTxNum, logger)
	logLatestAccountSample(tx, addr, logger)
	logRawHistorySeekSample(tx, addr, "firstUser", firstUserTxNum, logger)
	logRawHistorySeekSample(tx, addr, "current", currentTxNum, logger)
	logHistorySeekMeta(tx, addr, "firstUser", firstUserTxNum, logger)
	logHistorySeekMeta(tx, addr, "current", currentTxNum, logger)
	logHistoryFileLookup(tx, addr, "firstUser", firstUserTxNum, logger)
	logHistoryFileLookup(tx, addr, "current", currentTxNum, logger)
	if currentTxNum > 0 {
		logHistoricalAccountSample(tx, addr, "current_minus_1", currentTxNum-1, logger)
		logRawHistorySeekSample(tx, addr, "current_minus_1", currentTxNum-1, logger)
		logHistorySeekMeta(tx, addr, "current_minus_1", currentTxNum-1, logger)
		logHistoryFileLookup(tx, addr, "current_minus_1", currentTxNum-1, logger)
	}
	logLegacyShiftSamples(tx, blockNum, addr, "firstUser", firstUserTxNum, logger)
	logLegacyShiftSamples(tx, blockNum, addr, "current", currentTxNum, logger)
	logRawAccountTraceKey(ctx, tx, addr, currentTxNum+1, logger)
	logAccountTraceKey(ctx, tx, addr, currentTxNum+1, logger)
	return nil
}

func logBodySnapshotBoundaries(ctx context.Context, tx kv.TemporalTx, blockReader *freezeblocks.BlockReader, blockNum uint64, logger log.Logger) {
	if tx == nil || blockReader == nil {
		return
	}
	body, err := blockReader.CanonicalBodyForStorage(ctx, tx, blockNum)
	if err != nil {
		logger.Warn("[historical_account] body", "block", blockNum, "err", err)
		return
	}
	if body == nil {
		logger.Warn("[historical_account] body", "block", blockNum, "found", false)
		return
	}
	logger.Info("[historical_account] body",
		"block", blockNum,
		"baseTxnID", body.BaseTxnID.U64(),
		"firstUserTxNum", body.BaseTxnID.First(),
		"lastSystemTxNum", body.BaseTxnID.LastSystemTx(body.TxCount),
		"txCountWithSystem", body.TxCount,
		"userTxCount", body.TxCount-2,
	)
}

func logHistoricalAccountSample(tx kv.TemporalTx, addr accounts.Address, label string, txNum uint64, logger log.Logger) {
	reader := state.NewHistoryReaderV3(tx, txNum)
	acc, err := reader.ReadAccountData(addr)
	if err != nil {
		logger.Warn("[historical_account] sample",
			"label", label,
			"txNum", txNum,
			"address", addr,
			"err", err,
		)
		return
	}
	if acc == nil {
		logger.Warn("[historical_account] sample",
			"label", label,
			"txNum", txNum,
			"address", addr,
			"found", false,
		)
		return
	}
	logger.Info("[historical_account] sample",
		"label", label,
		"txNum", txNum,
		"address", addr,
		"found", true,
		"nonce", acc.Nonce,
		"balance", acc.Balance.ToBig().String(),
		"incarnation", acc.Incarnation,
		"codeHash", acc.CodeHash,
	)
}

func logLatestAccountSample(tx kv.TemporalTx, addr accounts.Address, logger log.Logger) {
	reader := state.NewReaderV3(tx)
	acc, err := reader.ReadAccountData(addr)
	if err != nil {
		logger.Warn("[historical_account] latest", "address", addr, "err", err)
		return
	}
	if acc == nil {
		logger.Warn("[historical_account] latest", "address", addr, "found", false)
		return
	}
	logger.Info("[historical_account] latest",
		"address", addr,
		"found", true,
		"nonce", acc.Nonce,
		"balance", acc.Balance.ToBig().String(),
		"incarnation", acc.Incarnation,
		"codeHash", acc.CodeHash,
	)
}

func logRawHistorySeekSample(tx kv.TemporalTx, addr accounts.Address, label string, txNum uint64, logger log.Logger) {
	aggTx := dbstate.AggTx(tx)
	if aggTx == nil {
		logger.Warn("[historical_account] raw_history_seek", "label", label, "txNum", txNum, "address", addr, "err", "no aggregator tx")
		return
	}
	key := addr.Value()
	v, ok, err := aggTx.HistorySeek(kv.AccountsDomain, key[:], txNum, tx)
	if err != nil {
		logger.Warn("[historical_account] raw_history_seek", "label", label, "txNum", txNum, "address", addr, "err", err)
		return
	}
	logger.Info("[historical_account] raw_history_seek",
		"label", label,
		"txNum", txNum,
		"address", addr,
		"found", ok,
		"valueLen", len(v),
		"valueHex", fmt.Sprintf("%x", v),
	)
}

func logHistorySeekMeta(tx kv.TemporalTx, addr accounts.Address, label string, txNum uint64, logger log.Logger) {
	aggTx := dbstate.AggTx(tx)
	if aggTx == nil {
		logger.Warn("[historical_account] history_seek_meta", "label", label, "txNum", txNum, "address", addr, "err", "no aggregator tx")
		return
	}
	key := addr.Value()
	histTxNum, v, ok, source, err := aggTx.DebugHistorySeekMeta(kv.AccountsDomain, key[:], txNum, tx)
	if err != nil {
		logger.Warn("[historical_account] history_seek_meta", "label", label, "txNum", txNum, "address", addr, "source", source, "err", err)
		return
	}
	logger.Info("[historical_account] history_seek_meta",
		"label", label,
		"txNum", txNum,
		"address", addr,
		"found", ok,
		"source", source,
		"histTxNum", histTxNum,
		"valueLen", len(v),
		"valueHex", fmt.Sprintf("%x", v),
	)
}

func logHistoryFileLookup(tx kv.TemporalTx, addr accounts.Address, label string, txNum uint64, logger log.Logger) {
	aggTx := dbstate.AggTx(tx)
	if aggTx == nil {
		logger.Warn("[historical_account] history_file_lookup", "label", label, "txNum", txNum, "address", addr, "err", "no aggregator tx")
		return
	}
	key := addr.Value()
	info, err := aggTx.DebugHistoryFileLookup(kv.AccountsDomain, key[:], txNum)
	if err != nil {
		logger.Warn("[historical_account] history_file_lookup", "label", label, "txNum", txNum, "address", addr, "err", err)
		return
	}
	if info == nil {
		logger.Warn("[historical_account] history_file_lookup", "label", label, "txNum", txNum, "address", addr, "found", false)
		return
	}
	fields := []any{
		"label", label,
		"txNum", txNum,
		"address", addr,
		"found", true,
		"histTxNum", info.HistTxNum,
		"file", info.File,
		"lookupOK", info.LookupOK,
		"lookupOffset", info.LookupOffset,
		"lookupValueLen", len(info.LookupValue),
		"lookup2OK", info.Lookup2OK,
		"lookup2Offset", info.Lookup2Offset,
		"lookup2ValueLen", len(info.Lookup2Value),
		"pageScanOK", info.PageScanOK,
		"pageScanValueLen", len(info.PageScanValue),
		"lookupHex", fmt.Sprintf("%x", info.LookupValue),
		"lookup2Hex", fmt.Sprintf("%x", info.Lookup2Value),
		"pageScanHex", fmt.Sprintf("%x", info.PageScanValue),
	}
	fields = appendDecodedAccountFields(fields, "lookup", info.LookupValue)
	fields = appendDecodedAccountFields(fields, "lookup2", info.Lookup2Value)
	fields = appendDecodedAccountFields(fields, "pageScan", info.PageScanValue)
	logger.Info("[historical_account] history_file_lookup", fields...)
	for i, entry := range info.PageEntries {
		entryFields := []any{
			"index", i,
			"keyHex", fmt.Sprintf("%x", entry.Key),
			"valueLen", len(entry.Value),
			"valueHex", fmt.Sprintf("%x", entry.Value),
		}
		entryFields = appendDecodedAccountFields(entryFields, fmt.Sprintf("pageEntry%d", i), entry.Value)
		logger.Info("[historical_account] history_file_lookup_page_entry", entryFields...)
	}
}

func logLegacyShiftSamples(tx kv.TemporalTx, blockNum uint64, addr accounts.Address, label string, txNum uint64, logger log.Logger) {
	if txNum > blockNum {
		shifted := txNum - blockNum
		logHistoricalAccountSample(tx, addr, label+"_minus_block", shifted, logger)
		logRawHistorySeekSample(tx, addr, label+"_minus_block", shifted, logger)
	}
	twiceBlock := blockNum * 2
	if txNum > twiceBlock {
		shifted := txNum - twiceBlock
		logHistoricalAccountSample(tx, addr, label+"_minus_2block", shifted, logger)
		logRawHistorySeekSample(tx, addr, label+"_minus_2block", shifted, logger)
	}
}

func logAccountTraceKey(ctx context.Context, tx kv.TemporalTx, addr accounts.Address, endTxNum uint64, logger log.Logger) {
	aggTx := dbstate.AggTx(tx)
	if aggTx == nil {
		logger.Warn("[historical_account] trace_key", "address", addr, "err", "no aggregator tx")
		return
	}
	key := addr.Value()
	it, err := aggTx.DebugTraceKey(ctx, kv.AccountsDomain, key[:], 0, endTxNum, tx)
	if err != nil {
		logger.Warn("[historical_account] trace_key", "address", addr, "endTxNum", endTxNum, "err", err)
		return
	}
	defer it.Close()

	const maxEntries = 64
	count := 0
	for it.HasNext() && count < maxEntries {
		txNum, val, nextErr := it.Next()
		if nextErr != nil {
			logger.Warn("[historical_account] trace_key", "address", addr, "endTxNum", endTxNum, "err", nextErr)
			return
		}
		fields := []any{
			"address", addr,
			"traceTxNum", txNum,
			"valueLen", len(val),
		}
		if len(val) > 0 {
			var acc accounts.Account
			if err := accounts.DeserialiseV3(&acc, val); err == nil {
				fields = append(fields,
					"nonce", acc.Nonce,
					"balance", acc.Balance.ToBig().String(),
					"incarnation", acc.Incarnation,
					"codeHash", acc.CodeHash,
				)
			} else {
				fields = append(fields, "decodeErr", err)
			}
		}
		logger.Info("[historical_account] trace_key", fields...)
		count++
	}
	logger.Info("[historical_account] trace_key_summary",
		"address", addr,
		"endTxNum", endTxNum,
		"logged", count,
		"truncated", it.HasNext(),
	)
}

func logRawAccountTraceKey(ctx context.Context, tx kv.TemporalTx, addr accounts.Address, endTxNum uint64, logger log.Logger) {
	aggTx := dbstate.AggTx(tx)
	if aggTx == nil {
		logger.Warn("[historical_account] raw_trace_key", "address", addr, "err", "no aggregator tx")
		return
	}
	key := addr.Value()
	it, err := aggTx.DebugRawHistoryTraceKey(ctx, kv.AccountsDomain, key[:], 0, endTxNum, tx)
	if err != nil {
		logger.Warn("[historical_account] raw_trace_key", "address", addr, "endTxNum", endTxNum, "err", err)
		return
	}
	defer it.Close()

	const maxEntries = 64
	count := 0
	for it.HasNext() && count < maxEntries {
		txNum, val, nextErr := it.Next()
		if nextErr != nil {
			logger.Warn("[historical_account] raw_trace_key", "address", addr, "endTxNum", endTxNum, "err", nextErr)
			return
		}
		fields := []any{
			"address", addr,
			"traceTxNum", txNum,
			"valueLen", len(val),
		}
		if len(val) > 0 {
			var acc accounts.Account
			if err := accounts.DeserialiseV3(&acc, val); err == nil {
				fields = append(fields,
					"nonce", acc.Nonce,
					"balance", acc.Balance.ToBig().String(),
					"incarnation", acc.Incarnation,
					"codeHash", acc.CodeHash,
				)
			} else {
				fields = append(fields, "decodeErr", err)
			}
		}
		logger.Info("[historical_account] raw_trace_key", fields...)
		count++
	}
	logger.Info("[historical_account] raw_trace_key_summary",
		"address", addr,
		"endTxNum", endTxNum,
		"logged", count,
		"truncated", it.HasNext(),
	)
}

func appendDecodedAccountFields(fields []any, prefix string, val []byte) []any {
	if len(val) == 0 {
		return fields
	}
	var acc accounts.Account
	if err := accounts.DeserialiseV3(&acc, val); err != nil {
		return append(fields, prefix+"DecodeErr", err)
	}
	return append(fields,
		prefix+"Nonce", acc.Nonce,
		prefix+"Balance", acc.Balance.ToBig().String(),
		prefix+"Incarnation", acc.Incarnation,
		prefix+"CodeHash", acc.CodeHash,
	)
}

func parseAddress(raw string) (accounts.Address, error) {
	if raw == "" {
		return accounts.NilAddress, fmt.Errorf("invalid hex address: %s", raw)
	}
	return accounts.InternAddress(common.HexToAddress(raw)), nil
}
