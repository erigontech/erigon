package commands

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
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
		label := kv.ChainDB
		db, err := openDB(dbCfg(label, path.Join(datadirCli, label.String())), true, logger)
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

		c, err := tx.Cursor(kv.HeaderTD)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer c.Close()

		out, err := os.Create(outputCsvFile)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		csvWriter := csv.NewWriter(out)
		if err = csvWriter.Write([]string{"source", "block_num", "block_hash", "td"}); err != nil {
			logger.Error(err.Error())
			return
		}

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

			td := new(big.Int)
			if err := rlp.Decode(bytes.NewReader(v), td); err != nil {
				logger.Error(err.Error())
				return
			}

			record := []string{"DB", strconv.FormatUint(blockNum, 10), blockHash.Hex(), td.String()}
			if err := csvWriter.Write(record); err != nil {
				logger.Error(err.Error())
				return
			}
		}
		if err != nil {
			logger.Error(err.Error())
			return
		}

		csvWriter.Flush()
		if err = csvWriter.Error(); err != nil {
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
		label := kv.ChainDB
		db, err := openDB(dbCfg(label, path.Join(datadirCli, label.String())), true, logger)
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
		snapDir := path.Join(datadirCli, "snapshots")
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

		out, err := os.Create(outputCsvFile)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		csvWriter := csv.NewWriter(out)
		err = csvWriter.Write([]string{"source", "id", "time_unix_sec", "date_time_utc", "tx_hash", "log_index"})
		if err != nil {
			logger.Error(err.Error())
			return
		}

		writeEventRow := func(source string, event *heimdall.EventRecordWithTime) error {
			return csvWriter.Write([]string{
				source,
				strconv.FormatUint(event.ID, 10),
				strconv.FormatInt(event.Time.Unix(), 10),
				event.Time.UTC().Format(time.RFC3339),
				event.TxHash.String(),
				strconv.FormatUint(event.LogIndex, 10),
			})
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

				if err = writeEventRow("SNAPSHOTS", &event); err != nil {
					logger.Error(err.Error())
					return
				}
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

			if err = writeEventRow("DB", &event); err != nil {
				logger.Error(err.Error())
				return
			}
		}
		if err != nil {
			logger.Error(err.Error())
			return
		}

		csvWriter.Flush()
		if err = csvWriter.Error(); err != nil {
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
		label := kv.ChainDB
		db, err := openDB(dbCfg(label, path.Join(datadirCli, label.String())), true, logger)
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

		snapDir := path.Join(datadirCli, "snapshots")
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

		out, err := os.Create(outputCsvFile)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		csvWriter := csv.NewWriter(out)
		err = csvWriter.Write([]string{"source", "block_num", "block_time_unix_sec", "block_date_time_utc", "events"})
		if err != nil {
			logger.Error(err.Error())
			return
		}

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

			var source string
			if blockNum <= lastFrozenEventBlockNum {
				source = "SNAPSHOTS"
			} else {
				source = "DB"
			}

			var eventsSb strings.Builder
			for _, eventBytes := range events {
				var event heimdall.EventRecordWithTime
				if err := event.UnmarshallBytes(eventBytes); err != nil {
					logger.Error(err.Error())
					return
				}

				eventsSb.WriteString("Event{ID:")
				eventsSb.WriteString(strconv.FormatUint(event.ID, 10))
				eventsSb.WriteString(",TimeUnixSec:")
				eventsSb.WriteString(strconv.FormatInt(event.Time.Unix(), 10))
				eventsSb.WriteString(",DateTimeUtc:")
				eventsSb.WriteString(event.Time.UTC().Format(time.RFC3339))
				eventsSb.WriteString("}")
				eventsSb.WriteRune(';')
			}

			record := []string{
				source,
				strconv.FormatUint(blockNum, 10),
				strconv.FormatUint(header.Time, 10),
				time.Unix(int64(header.Time), 0).UTC().Format(time.RFC3339),
				eventsSb.String(),
			}
			if err = csvWriter.Write(record); err != nil {
				logger.Error(err.Error())
				return
			}
		}

		csvWriter.Flush()
		if err = csvWriter.Error(); err != nil {
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
		label := kv.ChainDB
		db, err := openDB(dbCfg(label, path.Join(datadirCli, label.String())), true, logger)
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
		snapDir := path.Join(datadirCli, "snapshots")
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

		out, err := os.Create(outputCsvFile)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		csvWriter := csv.NewWriter(out)
		err = csvWriter.Write([]string{"source", "id", "start_block", "end_block", "selected_producers"})
		if err != nil {
			logger.Error(err.Error())
			return
		}

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

			var source string
			if spanId <= lastFrozenSpanId {
				source = "SNAPSHOTS"
			} else {
				source = "DB"
			}

			var selectedProducersSb strings.Builder
			for _, producer := range span.SelectedProducers {
				selectedProducersSb.WriteString(producer.String())
				selectedProducersSb.WriteRune(';')
			}

			record := []string{
				source,
				strconv.FormatUint(uint64(span.Id), 10),
				strconv.FormatUint(span.StartBlock, 10),
				strconv.FormatUint(span.EndBlock, 10),
				selectedProducersSb.String(),
			}

			if err = csvWriter.Write(record); err != nil {
				logger.Error(err.Error())
				return
			}
		}

		csvWriter.Flush()
		if err = csvWriter.Error(); err != nil {
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
		label := kv.ChainDB
		db, err := openDB(dbCfg(label, path.Join(datadirCli, label.String())), true, logger)
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

		out, err := os.Create(outputCsvFile)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		csvWriter := csv.NewWriter(out)
		if err = csvWriter.Write([]string{"source", "span_id", "start_block", "end_block", "producers"}); err != nil {
			logger.Error(err.Error())
			return
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, fromNum)
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

			var producersSb strings.Builder
			producers := sbps.Producers.Validators
			for _, producer := range producers {
				producersSb.WriteString(producer.String())
				producersSb.WriteRune(';')
			}

			record := []string{
				"DB",
				strconv.FormatUint(uint64(sbps.SpanId), 10),
				strconv.FormatUint(sbps.StartBlock, 10),
				strconv.FormatUint(sbps.EndBlock, 10),
				producersSb.String(),
			}

			if err = csvWriter.Write(record); err != nil {
				logger.Error(err.Error())
				return
			}
		}
		if err != nil {
			logger.Error(err.Error())
			return
		}

		csvWriter.Flush()
		if err = csvWriter.Error(); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}
