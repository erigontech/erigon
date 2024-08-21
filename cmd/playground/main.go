package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/polygoncommon"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const dataDir = "/Users/taratorio/erigon3-polygon-sync-stage-amoy-v5/datadir"
	const outputCsvFilePath = "/Users/taratorio/events-debugging.csv"
	const skipCsvOutputGen = false

	logger := log.New()
	consoleHandler := log.LvlFilterHandler(log.LvlDebug, log.StderrHandler)
	logger.SetHandler(consoleHandler)

	chainConfig := params.AmoyChainConfig
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	db := polygoncommon.NewDatabase(dataDir, kv.ChainDB, kv.ChaindataTablesCfg, logger)
	if err := db.OpenOnce(ctx); err != nil {
		panic(err)
	}

	defer db.Close()
	roTx, err := db.BeginRo(ctx)
	if err != nil {
		panic(err)
	}

	defer roTx.Rollback()

	snapDir := dataDir + "/snapshots"
	logger.Info("snapshot dir info", "dir", snapDir)

	allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{}, snapDir, 0, logger)
	if err := allSnapshots.ReopenFolder(); err != nil {
		panic(err)
	}

	allBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.BlocksFreezing{}, snapDir, 0, logger)
	if err := allBorSnapshots.ReopenFolder(); err != nil {
		panic(err)
	}

	blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)
	chainReader := consensuschain.NewReader(chainConfig, roTx, blockReader, logger)

	lastEventId, _, err := blockReader.LastEventId(ctx, roTx)
	if err != nil {
		panic(err)
	}

	borEventNumsCursor, err := roTx.Cursor(kv.BorEventNums)
	if err != nil {
		panic(err)
	}

	defer borEventNumsCursor.Close()

	_, lastBorEventNumsBytes, err := borEventNumsCursor.Last()
	if err != nil {
		panic(err)
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
	for blockNum := uint64(1); blockNum <= 6017264 && !skipCsvOutputGen; blockNum++ {
		sprintLen := borConfig.CalculateSprintLength(blockNum)
		if blockNum%sprintLen != 0 {
			continue
		}

		header := chainReader.GetHeaderByNumber(blockNum)
		if header == nil {
			panic(fmt.Sprintf("nil header: %d", blockNum))
		}

		events := chainReader.BorEventsByBlock(header.Hash(), blockNum)
		if len(events) == 0 {
			continue
		}

		var startEvent, endEvent heimdall.EventRecordWithTime
		if err := startEvent.UnmarshallBytes(events[0]); err != nil {
			panic(err)
		}
		if err := endEvent.UnmarshallBytes(events[len(events)-1]); err != nil {
			panic(err)
		}

		sb.WriteString(strconv.FormatUint(blockNum, 10))
		sb.WriteRune(',')
		sb.WriteString(strconv.FormatUint(startEvent.ID, 10))
		sb.WriteRune(',')
		sb.WriteString(strconv.FormatUint(endEvent.ID, 10))
		sb.WriteRune('\n')
	}

	if err := os.WriteFile(outputCsvFilePath, []byte(sb.String()), 0600); err != nil {
		panic(err)
	}
}
