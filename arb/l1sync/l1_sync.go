package l1sync

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/nitro-erigon/arbnode"
	"github.com/erigontech/nitro-erigon/arbstate/daprovider"
	"github.com/erigontech/nitro-erigon/execution"
	"github.com/erigontech/nitro-erigon/execution/erigon/ethclient"
	"github.com/erigontech/nitro-erigon/util/headerreader"
)

type L1SyncService struct {
	config         *Config
	sequencerInbox *arbnode.SequencerInbox
	delayedBridge  *arbnode.DelayedBridge
	l1Client       *ethclient.Client
	dapReaders     []daprovider.Reader
	exec           execution.ExecutionSequencer
	db             kv.RwDB
	logger         log.Logger

	delayedMessagesRead uint64

	ctx    context.Context
	cancel context.CancelFunc
}

func New(
	config *Config,
	l1Client *ethclient.Client,
	beaconUrl string,
	blobL1Client *ethclient.Client,
	exec execution.ExecutionSequencer,
	db kv.RwDB,
	logger log.Logger,
) (*L1SyncService, error) {
	seqInbox, err := arbnode.NewSequencerInbox(l1Client, config.SequencerInboxAddr, int64(config.StartL1Block))
	if err != nil {
		return nil, err
	}
	delayedBridge, err := arbnode.NewDelayedBridge(l1Client, config.BridgeAddr, config.StartL1Block)
	if err != nil {
		return nil, err
	}
	blobClient, err := headerreader.NewBlobClient(
		headerreader.BlobClientConfig{BeaconUrl: beaconUrl},
		blobL1Client)
	if err != nil {
		return nil, err
	}

	return &L1SyncService{
		config:         config,
		sequencerInbox: seqInbox,
		delayedBridge:  delayedBridge,
		l1Client:       l1Client,
		dapReaders:     []daprovider.Reader{daprovider.NewReaderForBlobReader(blobClient)},
		exec:           exec,
		db:             db,
		logger:         logger,
	}, nil
}

// fetchDelayedMessagesInRange fetches delayed messages from L1 for the given block range
// and stores them in DB.
func (s *L1SyncService) fetchDelayedMessagesInRange(ctx context.Context, fromL1Block, toL1Block uint64) error {
	from := new(big.Int).SetUint64(fromL1Block)
	to := new(big.Int).SetUint64(toL1Block)
	delayedMsgs, err := s.delayedBridge.LookupMessagesInRange(ctx, from, to, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch delayed messages in L1 range [%d, %d]: %w", fromL1Block, toL1Block, err)
	}
	if len(delayedMsgs) == 0 {
		return nil
	}

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin DB transaction for delayed messages: %w", err)
	}
	defer tx.Rollback()

	for _, dm := range delayedMsgs {
		seqNum, err := dm.Message.Header.SeqNum()
		if err != nil {
			return fmt.Errorf("failed to get delayed message seqNum: %w", err)
		}
		if err := putDelayedMessage(tx, seqNum, dm.Message); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit delayed messages: %w", err)
	}
	s.logger.Info("stored delayed messages", "count", len(delayedMsgs), "fromL1Block", fromL1Block, "toL1Block", toL1Block)
	return nil
}

func (s *L1SyncService) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Load delayedMessagesRead from DB (from the last message of the last processed batch)
	lastBatch, _, err := s.GetProgress(ctx)
	if err == nil && lastBatch > 0 {
		dmr, err := s.getLastDelayedMessagesRead(ctx, lastBatch)
		if err == nil {
			s.delayedMessagesRead = dmr
		}
	}

	go s.pollLoop()
}

func (s *L1SyncService) StopAndWait() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *L1SyncService) pollLoop() {
	s.logger.Info("L1 sync service started")
	for {
		if s.ctx.Err() != nil {
			s.logger.Info("L1 sync service stopped")
			return
		}

		pollMore, err := s.pollOnce(s.ctx)
		if err != nil {
			s.logger.Warn("L1 sync poll error", "err", err)
		}

		if pollMore {
			// Still catching up — poll again immediately
			continue
		}

		// At chain tip — wait before polling again
		select {
		case <-s.ctx.Done():
			s.logger.Info("L1 sync service stopped")
			return
		case <-time.After(s.config.PollInterval):
		}
	}
}

// pollOnce processes available batches. Returns pollMore=true if there may be
// more batches to process (caller should re-poll immediately instead of sleeping).
func (s *L1SyncService) pollOnce(ctx context.Context) (pollMore bool, err error) {
	lastBatchSeqNum, lastL1Block, err := s.GetProgress(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to read progress: %w", err)
	}

	fromL1Block := lastL1Block
	if fromL1Block == 0 {
		if s.config.StartL1Block > 0 {
			fromL1Block = s.config.StartL1Block
		} else {
			s.logger.Debug("no progress and no start-l1-block configured, waiting")
			return false, nil
		}
	} else {
		fromL1Block++
	}

	currentL1Block, err := s.l1Client.BlockNumber(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get L1 block number: %w", err)
	}

	if fromL1Block > currentL1Block {
		s.logger.Debug("caught up with L1", "lastL1Block", lastL1Block, "currentL1Block", currentL1Block)
		return false, nil
	}

	s.logger.Info("polling L1 for batches", "fromL1Block", fromL1Block, "currentL1Block", currentL1Block, "lastBatchSeqNum", lastBatchSeqNum)

	// Process in chunks
	pollStart := time.Now()
	startL1Block := fromL1Block
	batchesProcessed := uint64(0)
	for fromL1Block <= currentL1Block {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		toL1Block := fromL1Block + s.config.L1BlocksPerRequest - 1
		if toL1Block > currentL1Block {
			toL1Block = currentL1Block
		}

		// Fetch delayed messages for this chunk and accumulate them
		if err := s.fetchDelayedMessagesInRange(ctx, fromL1Block, toL1Block); err != nil {
			s.logger.Warn("failed to fetch delayed messages for chunk", "fromL1Block", fromL1Block, "toL1Block", toL1Block, "err", err)
		}

		batches, err := s.FetchBatchesInRange(ctx, fromL1Block, toL1Block)
		if err != nil {
			return false, err
		}

		for _, batch := range batches {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			
			// skip batches we already processed
			if batch.SequenceNumber <= lastBatchSeqNum && lastBatchSeqNum > 0 {
				continue
			}

			data, err := s.FetchAndSerializeBatch(ctx, batch)
			if err != nil {
				return false, err
			}

			if err := s.ProcessBatch(ctx, batch.SequenceNumber, data, batch.BlockHash, batch.ParentChainBlockNumber); err != nil {
				return false, fmt.Errorf("failed to process batch %d: %w", batch.SequenceNumber, err)
			}

			batchesProcessed++
			if s.config.MaxBatchesPerPoll > 0 && batchesProcessed >= s.config.MaxBatchesPerPoll {
				elapsed := time.Since(pollStart)
				l1Blocks := fromL1Block - startL1Block + 1
				s.logger.Info("reached max batches per poll",
					"processed", batchesProcessed,
					"l1Blocks", l1Blocks,
					"elapsed", elapsed,
					"l1Blk/s", fmt.Sprintf("%.1f", float64(l1Blocks)/elapsed.Seconds()),
					"batch/s", fmt.Sprintf("%.1f", float64(batchesProcessed)/elapsed.Seconds()),
				)
				return true, nil // need to poll more to sync up to chain tip
			}
		}

		fromL1Block = toL1Block + 1
	}

	if batchesProcessed > 0 {
		elapsed := time.Since(pollStart)
		l1Blocks := currentL1Block - startL1Block + 1
		s.logger.Info("poll cycle complete",
			"batchesProcessed", batchesProcessed,
			"l1Blocks", l1Blocks,
			"elapsed", elapsed,
			"l1Blk/s", fmt.Sprintf("%.1f", float64(l1Blocks)/elapsed.Seconds()),
			"batch/s", fmt.Sprintf("%.1f", float64(batchesProcessed)/elapsed.Seconds()),
		)
	}
	return false, nil // processed everything up to current L1 chain tip
}

// FetchAndProcessRange fetches and processes all batches in the given L1 block range (manual call, not continuous polling)
func (s *L1SyncService) FetchAndProcessRange(ctx context.Context, fromL1Block, toL1Block uint64) error {
	s.logger.Info("fetching and processing range", "fromL1Block", fromL1Block, "toL1Block", toL1Block)

	for from := fromL1Block; from <= toL1Block; {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		to := from + s.config.L1BlocksPerRequest - 1
		if to > toL1Block {
			to = toL1Block
		}

		// Fetch delayed messages for this chunk and accumulate them
		if err := s.fetchDelayedMessagesInRange(ctx, from, to); err != nil {
			s.logger.Warn("failed to fetch delayed messages for chunk", "fromL1Block", from, "toL1Block", to, "err", err)
		}

		batches, err := s.FetchBatchesInRange(ctx, from, to)
		if err != nil {
			return err
		}

		for _, batch := range batches {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			data, err := s.FetchAndSerializeBatch(ctx, batch)
			if err != nil {
				return err
			}

			if err := s.ProcessBatch(ctx, batch.SequenceNumber, data, batch.BlockHash, batch.ParentChainBlockNumber); err != nil {
				return fmt.Errorf("failed to process batch %d: %w", batch.SequenceNumber, err)
			}
		}

		from = to + 1
	}

	return nil
}
