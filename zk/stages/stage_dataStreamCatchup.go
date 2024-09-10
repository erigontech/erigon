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
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/log/v3"
)

type DataStreamCatchupCfg struct {
	db            kv.RwDB
	stream        *datastreamer.StreamServer
	chainId       uint64
	streamVersion int
	hasExecutors  bool
}

func StageDataStreamCatchupCfg(stream *datastreamer.StreamServer, db kv.RwDB, chainId uint64, streamVersion int, hasExecutors bool) DataStreamCatchupCfg {
	return DataStreamCatchupCfg{
		stream:        stream,
		db:            db,
		chainId:       chainId,
		streamVersion: streamVersion,
		hasExecutors:  hasExecutors,
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

	finalBlockNumber, err := CatchupDatastream(ctx, logPrefix, tx, stream, cfg.chainId)
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

func CatchupDatastream(ctx context.Context, logPrefix string, tx kv.RwTx, stream *datastreamer.StreamServer, chainId uint64) (uint64, error) {
	srv := server.NewDataStreamServer(stream, chainId)
	reader := hermez_db.NewHermezDbReader(tx)

	var (
		err              error
		finalBlockNumber uint64
	)

	if sequencer.IsSequencer() {
		finalBlockNumber, err = stages.GetStageProgress(tx, stages.DataStream)
		if err != nil {
			return 0, err
		}

		// this handles a case where the node was synced in RPC mode without a datastream
		// in this case the datastream progress will be 0 but it still should catch up
		if finalBlockNumber == 0 {
			finalBlockNumber, err = stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return 0, err
			}
		}
	} else {
		finalBlockNumber, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return 0, err
		}
	}

	previousProgress, err := srv.GetHighestBlockNumber()
	if err != nil {
		return 0, err
	}

	log.Info(fmt.Sprintf("[%s] Getting progress", logPrefix),
		"adding up to blockNum", finalBlockNumber,
		"previousProgress", previousProgress,
	)

	// write genesis if we have no data in the stream yet
	if previousProgress == 0 {
		// a quick check that we haven't written anything to the stream yet.  Stage progress is a little misleading
		// for genesis as we are in fact at block 0 here!  Getting the header has some performance overhead, so
		// we only want to do this when we know the previous progress is 0.
		header := stream.GetHeader()
		if header.TotalEntries == 0 {
			genesis, err := rawdb.ReadBlockByNumber(tx, 0)
			if err != nil {
				return 0, err
			}
			if err = srv.WriteGenesisToStream(genesis, reader, tx); err != nil {
				return 0, err
			}
		}
	}

	if err = srv.WriteBlocksToStreamConsecutively(ctx, logPrefix, tx, reader, previousProgress+1, finalBlockNumber); err != nil {
		return 0, err
	}

	if err = stages.SaveStageProgress(tx, stages.DataStream, finalBlockNumber); err != nil {
		return 0, err
	}

	return finalBlockNumber, nil
}
