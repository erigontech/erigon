package commands

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/snaptype"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var cmdExportHeaderTd = &cobra.Command{
	Use:   "export_header_td",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, datadirCli+"/chaindata"), true, logger)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer db.Close()
		tx, err := db.BeginRo(cmd.Context())
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer tx.Rollback()

		var sb strings.Builder
		c, err := tx.Cursor(kv.HeaderTD)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer c.Close()
		var k, v []byte
		for k, v, err = c.First(); err == nil && k != nil; k, v, err = c.Next() {
			blockNum := binary.BigEndian.Uint64(k[:8])
			if blockNum < fromNum {
				continue
			}

			if blockNum >= toNum {
				break
			}

			var blockHash libcommon.Hash
			copy(blockHash[8:], k)

			sb.WriteString(fmt.Sprintf("%d", blockNum))
			sb.WriteRune(',')

			sb.WriteString(blockHash.Hex())
			sb.WriteRune(',')

			td := new(big.Int)
			if err := rlp.Decode(bytes.NewReader(v), td); err != nil {
				logger.Error(err.Error())
				return
			}

			sb.WriteString(td.String())
			sb.WriteRune(',')

			sb.WriteString(common.Bytes2Hex(k))
			sb.WriteRune('\n')
		}
		if err != nil {
			logger.Error(err.Error())
			return
		}

		if err = os.WriteFile(outputCsvFile, []byte(sb.String()), 0600); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}

var cmdExportHeimdallEvents = &cobra.Command{
	Use:   "export_heimdall_events",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, datadirCli+"/chaindata"), true, logger)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer db.Close()
		tx, err := db.BeginRo(cmd.Context())
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer tx.Rollback()
		snapDir := datadirCli + "/snapshots"
		logger.Info("snapshot dir info", "dir", snapDir)

		allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{}, snapDir, 0, logger)
		if err := allSnapshots.ReopenFolder(); err != nil {
			logger.Error(err.Error())
			return
		}

		allBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.BlocksFreezing{}, snapDir, 0, logger)
		if err := allBorSnapshots.ReopenFolder(); err != nil {
			logger.Error(err.Error())
			return
		}

		eventSegments := allBorSnapshots.ViewType(snaptype.BorEvents).VisibleSegments
		blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)
		lastFrozenEventId := blockReader.LastFrozenEventId()
		iterateSnapshots := lastFrozenEventId > 0 && fromNum <= lastFrozenEventId
		iterateDb := true

		var sb strings.Builder
		writeEventRow := func(event *heimdall.EventRecordWithTime, source string) {
			sb.WriteString(strconv.FormatUint(event.ID, 10))
			sb.WriteRune(',')
			sb.WriteString(strconv.FormatInt(event.Time.Unix(), 10))
			sb.WriteRune(',')
			sb.WriteString(event.Time.Format(time.RFC3339))
			sb.WriteRune(',')
			sb.WriteString(event.TxHash.String())
			sb.WriteRune(',')
			sb.WriteString(strconv.FormatUint(event.LogIndex, 10))
			sb.WriteRune(',')
			sb.WriteString(source)
			sb.WriteRune('\n')
		}

	snapshotLoop:
		for i := 0; iterateSnapshots && i < len(eventSegments); i++ {
			seg := eventSegments[i]
			getter := seg.MakeGetter()
			for getter.HasNext() {
				var buf []byte
				buf, _ = getter.Next(buf)

				eventBytes := rlp.RawValue(libcommon.Copy(buf[length.Hash+length.BlockNum+8:]))
				var event heimdall.EventRecordWithTime
				if err := event.UnmarshallBytes(eventBytes); err != nil {
					logger.Error(err.Error())
					return
				}

				if event.ID < fromNum {
					continue
				}

				if event.ID >= toNum {
					iterateDb = false
					break snapshotLoop
				}

				writeEventRow(&event, "SNAPSHOTS")
			}
		}

		c, err := tx.Cursor(kv.BorEvents)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer c.Close()
		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, max(fromNum, lastFrozenEventId+1))
		var k, v []byte
		for k, v, err = c.Seek(from); iterateDb && err == nil && k != nil; k, v, err = c.Next() {
			eventId := binary.BigEndian.Uint64(k)
			if eventId >= toNum {
				break
			}

			var event heimdall.EventRecordWithTime
			if err := event.UnmarshallBytes(v); err != nil {
				logger.Error(err.Error())
				return
			}

			sb.WriteString(strconv.FormatUint(eventId, 10))
			sb.WriteRune(',')
			writeEventRow(&event, "DB")
		}
		if err != nil {
			logger.Error(err.Error())
			return
		}

		if err := os.WriteFile(outputCsvFile, []byte(sb.String()), 0600); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}

var cmdExportHeimdallEventsPerBlock = &cobra.Command{
	Use:   "export_heimdall_events_per_block",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, datadirCli+"/chaindata"), true, logger)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer db.Close()
		tx, err := db.BeginRo(cmd.Context())
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer tx.Rollback()

		snapDir := datadirCli + "/snapshots"
		logger.Info("snapshot dir info", "dir", snapDir)

		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		borConfig := chainConfig.Bor.(*borcfg.BorConfig)
		allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{}, snapDir, 0, logger)
		if err := allSnapshots.ReopenFolder(); err != nil {
			logger.Error(err.Error())
			return
		}

		allBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.BlocksFreezing{}, snapDir, 0, logger)
		if err := allBorSnapshots.ReopenFolder(); err != nil {
			logger.Error(err.Error())
			return
		}

		blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)
		chainReader := consensuschain.NewReader(chainConfig, tx, blockReader, logger)

		lastFrozenEventBlockNum := blockReader.LastFrozenEventBlockNum()
		currentBlock, err := blockReader.CurrentBlock(tx)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		var sb strings.Builder
		to := min(currentBlock.NumberU64()+1, toNum)
		for blockNum := fromNum; blockNum < to; blockNum++ {
			sprintLen := borConfig.CalculateSprintLength(blockNum)
			if blockNum%sprintLen != 0 {
				continue
			}

			header := chainReader.GetHeaderByNumber(blockNum)
			if header == nil {
				logger.Error("nil header", "blockNum", blockNum)
				return
			}

			events := chainReader.BorEventsByBlock(header.Hash(), blockNum)
			if len(events) == 0 {
				continue
			}

			sb.WriteString(strconv.FormatUint(blockNum, 10))
			sb.WriteRune(',')
			sb.WriteString(strconv.FormatUint(header.Time, 10))
			sb.WriteRune(',')
			sb.WriteString(time.Unix(int64(header.Time), 0).Format(time.RFC3339))
			sb.WriteRune(',')

			for i, eventBytes := range events {
				var event heimdall.EventRecordWithTime
				if err := event.UnmarshallBytes(eventBytes); err != nil {
					logger.Error(err.Error())
					return
				}

				sb.WriteString(strconv.FormatUint(event.ID, 10))
				sb.WriteRune('_')
				sb.WriteString(strconv.FormatInt(event.Time.Unix(), 10))
				sb.WriteRune('_')
				sb.WriteString(event.Time.Format(time.RFC3339))

				if i < len(events)-1 {
					sb.WriteRune(';')
				}
			}

			var source string
			if blockNum <= lastFrozenEventBlockNum {
				source = "SNAPSHOTS"
			} else {
				source = "DB"
			}

			sb.WriteRune(',')
			sb.WriteString(source)
			sb.WriteRune('\n')
		}

		if err := os.WriteFile(outputCsvFile, []byte(sb.String()), 0600); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}

var cmdExportHeimdallSpans = &cobra.Command{
	Use:   "export_heimdall_spans",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, datadirCli+"/chaindata"), true, logger)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer db.Close()
		tx, err := db.BeginRo(cmd.Context())
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer tx.Rollback()
		snapDir := datadirCli + "/snapshots"
		logger.Info("snapshot dir info", "dir", snapDir)

		allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{}, snapDir, 0, logger)
		if err := allSnapshots.ReopenFolder(); err != nil {
			logger.Error(err.Error())
			return
		}

		allBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.BlocksFreezing{}, snapDir, 0, logger)
		if err := allBorSnapshots.ReopenFolder(); err != nil {
			logger.Error(err.Error())
			return
		}

		blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)
		lastFrozenSpanId := blockReader.LastFrozenSpanId()

		var to uint64
		lastSpanId, ok, err := blockReader.LastSpanId(cmd.Context(), tx)
		if err != nil {
			logger.Error(err.Error())
			return
		}
		if ok {
			to = min(lastSpanId+1, toNum)
		}

		var sb strings.Builder
		for spanId := fromNum; spanId < to; spanId++ {
			spanBytes, err := blockReader.Span(cmd.Context(), tx, spanId)
			if err != nil {
				if errors.Is(err, freezeblocks.ErrSpanNotFound) {
					logger.Warn("span not found", "spanId", spanId)
					continue
				}

				logger.Error(err.Error())
				return
			}

			var span heimdall.Span
			if err = json.Unmarshal(spanBytes, &span); err != nil {
				logger.Error(err.Error())
				return
			}

			sb.WriteString(strconv.FormatUint(uint64(span.Id), 10))
			sb.WriteRune(',')
			sb.WriteString(strconv.FormatUint(span.StartBlock, 10))
			sb.WriteRune(',')
			sb.WriteString(strconv.FormatUint(span.EndBlock, 10))
			sb.WriteRune(',')

			for i, producer := range span.SelectedProducers {
				sb.WriteString(producer.String())
				if i < len(span.SelectedProducers)-1 {
					sb.WriteRune(';')
				}
			}

			if len(span.SelectedProducers) > 0 {
				sb.WriteRune(',')
			}

			var source string
			if spanId <= lastFrozenSpanId {
				source = "SNAPSHOTS"
			} else {
				source = "DB"
			}

			sb.WriteString(source)
			sb.WriteRune('\n')
		}

		if err := os.WriteFile(outputCsvFile, []byte(sb.String()), 0600); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}

var cmdExportHeimdallSpanBlockProducerSelections = &cobra.Command{
	Use:   "export_heimdall_span_block_producer_selections",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, datadirCli+"/chaindata"), true, logger)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer db.Close()
		tx, err := db.BeginRo(cmd.Context())
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer tx.Rollback()

		c, err := tx.Cursor(kv.BorProducerSelections)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer c.Close()
		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, fromNum)
		var sb strings.Builder
		var k, v []byte
		for k, v, err = c.Seek(from); err == nil && k != nil; k, v, err = c.Next() {
			if binary.BigEndian.Uint64(k) >= toNum {
				break
			}

			var sbps heimdall.SpanBlockProducerSelection
			if err = json.Unmarshal(v, &sbps); err != nil {
				logger.Error(err.Error())
				return
			}

			sb.WriteString(strconv.FormatUint(uint64(sbps.SpanId), 10))
			sb.WriteRune(',')
			sb.WriteString(strconv.FormatUint(sbps.StartBlock, 10))
			sb.WriteRune(',')
			sb.WriteString(strconv.FormatUint(sbps.EndBlock, 10))

			producers := sbps.Producers.Validators
			if len(producers) > 0 {
				sb.WriteRune(',')
			}

			for i, producer := range producers {
				sb.WriteString(producer.String())
				if i < len(producers)-1 {
					sb.WriteRune(';')
				}
			}

			sb.WriteRune('\n')
		}
		if err != nil {
			logger.Error(err.Error())
			return
		}

		if err = os.WriteFile(outputCsvFile, []byte(sb.String()), 0600); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}
