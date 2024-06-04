package stages

import (
	"context"
	"fmt"
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
	"github.com/ledgerwatch/erigon/zk/sequencer"
)

type DataStreamCatchupCfg struct {
	db            kv.RwDB
	stream        *datastreamer.StreamServer
	chainId       uint64
	streamVersion int
}

func StageDataStreamCatchupCfg(stream *datastreamer.StreamServer, db kv.RwDB, chainId uint64, streamVersion int) DataStreamCatchupCfg {
	return DataStreamCatchupCfg{
		stream:        stream,
		db:            db,
		chainId:       chainId,
		streamVersion: streamVersion,
	}
}

func SpawnStageDataStreamCatchup(
	s *stagedsync.StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg DataStreamCatchupCfg,
) error {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting...", logPrefix))
	stream := cfg.stream

	if stream == nil {
		// skip the stage if there is no streamer provided
		log.Info(fmt.Sprintf("[%s] no streamer provided, skipping stage", logPrefix))
		return nil
	}

	createdTx := false
	if tx == nil {
		log.Debug(fmt.Sprintf("[%s] data stream: no tx provided, creating a new one", logPrefix))
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("failed to open tx, %w", err)
		}
		defer tx.Rollback()
		createdTx = true
	}

	finalBlockNumber, err := CatchupDatastream(logPrefix, tx, stream, cfg.chainId, cfg.streamVersion)
	if err != nil {
		return err
	}

	if createdTx {
		if err := tx.Commit(); err != nil {
			log.Error(fmt.Sprintf("[%s] error: %s", logPrefix, err))
		}
	}

	log.Info(fmt.Sprintf("[%s] stage complete", logPrefix), "block", finalBlockNumber)

	return err
}

func CatchupDatastream(logPrefix string, tx kv.RwTx, stream *datastreamer.StreamServer, chainId uint64, streamVersion int) (uint64, error) {
	srv := server.NewDataStreamServer(stream, chainId)
	reader := hermez_db.NewHermezDbReader(tx)

	// get the latest verified batch number
	var err error
	var latestBatch uint64
	var finalBlockNumber uint64
	if sequencer.IsSequencer() {
		latestBatch, err = stages.GetStageProgress(tx, stages.SequenceExecutorVerify)
		if err != nil {
			return 0, err
		}
		finalBlockNumber, err = reader.GetHighestBlockInBatch(latestBatch)
		if err != nil {
			return 0, err
		}
	} else {
		finalBlockNumber, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return 0, err
		}
	}

	previousProgress, err := stages.GetStageProgress(tx, stages.DataStream)
	if err != nil {
		return 0, err
	}

	log.Info(fmt.Sprintf("[%s] Getting progress", logPrefix),
		"adding up to blockNum", finalBlockNumber,
		"previousProgress", previousProgress,
	)

	// write genesis if we have no data in the stream yet
	if previousProgress == 0 {
		genesis, err := rawdb.ReadBlockByNumber(tx, 0)
		if err != nil {
			return 0, err
		}
		if err = server.WriteGenesisToStream(genesis, reader, stream, srv, chainId); err != nil {
			return 0, err
		}
	}

	if err = server.WriteBlocksToStream(tx, reader, srv, stream, previousProgress+1, finalBlockNumber, logPrefix); err != nil {
		return 0, err
	}

	if err = stages.SaveStageProgress(tx, stages.DataStream, finalBlockNumber); err != nil {
		return 0, err
	}

	return finalBlockNumber, nil
}
