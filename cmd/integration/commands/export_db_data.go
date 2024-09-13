package commands

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
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
		db, err := openDB(dbCfg(kv.ChainDB, datadirCli), true, logger)
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

var cmdExportHeimdallEventsPerBlock = &cobra.Command{
	Use:   "export_heimdall_events_per_block",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, datadirCli), true, logger)
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

		lastEventId, _, err := blockReader.LastEventId(cmd.Context(), tx)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		c, err := tx.Cursor(kv.BorEventNums)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer c.Close()

		_, lastBorEventNumsBytes, err := c.Last()
		if err != nil {
			logger.Error(err.Error())
			return
		}

		var lastBorEventNums uint64
		if len(lastBorEventNumsBytes) > 0 {
			lastBorEventNums = binary.BigEndian.Uint64(lastBorEventNumsBytes)
		}

		logger.Info(
			"bor snapshot info",
			"LastFrozenEventId", blockReader.LastFrozenEventId(),
			"LastEventId", lastEventId,
			"LastBorEventNums", lastBorEventNums,
		)

		var sb strings.Builder
		for blockNum := fromNum; blockNum < toNum; blockNum++ {
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
			sb.WriteRune(';')
			sb.WriteString(time.Unix(int64(header.Time), 0).Format(time.RFC3339))
			sb.WriteRune(',')

			for _, eventBytes := range events {
				var event heimdall.EventRecordWithTime
				if err := event.UnmarshallBytes(eventBytes); err != nil {
					logger.Error(err.Error())
					return
				}

				sb.WriteString(strconv.FormatUint(event.ID, 10))
				sb.WriteRune(';')
				sb.WriteString(event.Time.Format(time.RFC3339))
				sb.WriteRune(';')
			}

			sb.WriteRune('\n')
		}

		if err := os.WriteFile(outputCsvFile, []byte(sb.String()), 0600); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}
