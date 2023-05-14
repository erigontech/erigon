package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"
	rawdb2 "github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	execution_client2 "github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/network"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/log/v3"
)

type StageHistoryReconstructionCfg struct {
	db              kv.RwDB
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig
	downloader      *network.BackwardBeaconDownloader
	state           *state.BeaconState
	executionClient *execution_client2.ExecutionClient
	beaconDBCfg     *rawdb2.BeaconDataConfig
	tmpdir          string
}

const logIntervalTime = 30 * time.Second

func StageHistoryReconstruction(db kv.RwDB, downloader *network.BackwardBeaconDownloader, genesisCfg *clparams.GenesisConfig, beaconCfg *clparams.BeaconChainConfig, beaconDBCfg *rawdb2.BeaconDataConfig, state *state.BeaconState, tmpdir string, executionClient *execution_client2.ExecutionClient) StageHistoryReconstructionCfg {
	return StageHistoryReconstructionCfg{
		db:              db,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		downloader:      downloader,
		state:           state,
		tmpdir:          tmpdir,
		executionClient: executionClient,
		beaconDBCfg:     beaconDBCfg,
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageHistoryReconstruction(cfg StageHistoryReconstructionCfg, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context) error {
	// This stage must be done only once.
	progress := s.BlockNumber
	if progress != 0 {
		return nil
	}

	useExternalTx := tx != nil
	var err error
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	blockRoot, err := cfg.state.BlockRoot()
	if err != nil {
		return err
	}
	destinationSlot := uint64(0)
	currentSlot := cfg.state.LatestBlockHeader().Slot
	if currentSlot > cfg.beaconDBCfg.BackFillingAmount {
		destinationSlot = currentSlot - cfg.beaconDBCfg.BackFillingAmount
	}

	// ETL collectors for attestations + beacon blocks
	beaconBlocksCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer beaconBlocksCollector.Close()
	attestationsCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer attestationsCollector.Close()
	executionPayloadsCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer executionPayloadsCollector.Close()
	// Indexes collector
	rootToSlotCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer rootToSlotCollector.Close()
	// Lastly finalizations markers collector.
	finalizationCollector := etl.NewCollector(s.LogPrefix(), cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer finalizationCollector.Close()
	// Start the procedure
	log.Info(fmt.Sprintf("[%s] Reconstructing", s.LogPrefix()), "from", cfg.state.LatestBlockHeader().Slot, "to", destinationSlot)
	// Setup slot and block root
	cfg.downloader.SetSlotToDownload(currentSlot)
	cfg.downloader.SetExpectedRoot(blockRoot)
	foundLatestEth1ValidHash := false
	if cfg.executionClient == nil {
		foundLatestEth1ValidHash = true
	}
	// Set up onNewBlock callback
	cfg.downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock) (finished bool, err error) {
		slot := blk.Block.Slot
		blockRoot, err := blk.Block.HashSSZ()
		if err != nil {
			return false, err
		}
		key := append(rawdb2.EncodeNumber(slot), blockRoot[:]...)
		// Collect attestations
		encodedAttestations, err := rawdb.EncodeAttestationsForStorage(blk.Block.Body.Attestations)
		if err != nil {
			return false, err
		}
		if err := attestationsCollector.Collect(key, encodedAttestations); err != nil {
			return false, err
		}
		// Collect beacon blocks
		encodedBeaconBlock, err := blk.EncodeForStorage()
		if err != nil {
			return false, err
		}
		slotBytes := rawdb2.EncodeNumber(slot)
		if err := beaconBlocksCollector.Collect(key, encodedBeaconBlock); err != nil {
			return false, err
		}
		// Collect hashes
		if err := rootToSlotCollector.Collect(blockRoot[:], slotBytes); err != nil {
			return false, err
		}
		if err := rootToSlotCollector.Collect(blk.Block.StateRoot[:], slotBytes); err != nil {
			return false, err
		}
		// Mark finalization markers.
		if err := finalizationCollector.Collect(slotBytes, blockRoot[:]); err != nil {
			return false, err
		}
		// Collect Execution Payloads
		if blk.Version() >= clparams.BellatrixVersion && !foundLatestEth1ValidHash {
			payload := blk.Block.Body.ExecutionPayload
			if foundLatestEth1ValidHash, err = cfg.executionClient.IsCanonical(payload.BlockHash); err != nil {
				return false, err
			}
			if foundLatestEth1ValidHash {
				return slot <= destinationSlot, nil
			}
			encodedPayload := make([]byte, 0, payload.EncodingSizeSSZ())
			encodedPayload, err = payload.EncodeSSZ(encodedPayload)
			if err != nil {
				return false, err
			}
			if err := executionPayloadsCollector.Collect(rawdb2.EncodeNumber(slot), encodedPayload); err != nil {
				return false, err
			}
		}
		return slot <= destinationSlot && foundLatestEth1ValidHash, nil
	})
	prevProgress := cfg.downloader.Progress()

	logInterval := time.NewTicker(logIntervalTime)
	finishCh := make(chan struct{})
	// Start logging thread
	go func() {
		for {
			select {
			case <-logInterval.C:
				logArgs := []interface{}{}
				currProgress := cfg.downloader.Progress()
				speed := float64(prevProgress-currProgress) / float64(logIntervalTime/time.Second)
				prevProgress = currProgress
				peerCount, err := cfg.downloader.Peers()
				if err != nil {
					return
				}
				logArgs = append(logArgs,
					"progress", currProgress,
					"blk/sec", fmt.Sprintf("%.1f", speed),
					"peers", peerCount)
				if currentSlot > destinationSlot {
					logArgs = append(logArgs, "remaining", currProgress-destinationSlot)
				}
				log.Info(fmt.Sprintf("[%s] Backwards downloading phase", s.LogPrefix()), logArgs...)
			case <-finishCh:
				return
			case <-ctx.Done():

			}
		}
	}()
	for !cfg.downloader.Finished() {
		cfg.downloader.RequestMore()
	}
	close(finishCh)
	if err := attestationsCollector.Load(tx, kv.Attestetations, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return err
	}
	if err := beaconBlocksCollector.Load(tx, kv.BeaconBlocks, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return err
	}
	if err := rootToSlotCollector.Load(tx, kv.RootSlotIndex, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := finalizationCollector.Load(tx, kv.FinalizedBlockRoots, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	executionPayloadInsertionBatch := execution_client2.NewInsertBatch(cfg.executionClient)
	// Send in ordered manner EL blocks to Execution Layer
	if err := executionPayloadsCollector.Load(tx, kv.BeaconBlocks, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		payload := &cltypes.Eth1Block{}
		if err := payload.DecodeSSZ(v, int(clparams.BellatrixVersion)); err != nil {
			return err
		}
		if err := executionPayloadInsertionBatch.WriteExecutionPayload(payload); err != nil {
			return err
		}
		return next(k, nil, nil)
	}, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return err
	}
	if err := executionPayloadInsertionBatch.Flush(); err != nil {
		return err
	}
	if err := s.Update(tx, 1); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
