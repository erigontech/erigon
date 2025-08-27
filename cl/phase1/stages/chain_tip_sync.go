package stages

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/sentinel/peers"
)

// waitForExecutionEngineToBeFinished checks if the execution engine is ready within a specified timeout.
// It periodically checks the readiness of the execution client and returns true if the client is ready before
// the timeout occurs. If the context is canceled or a timeout occurs, it returns false with the corresponding error.
func waitForExecutionEngineToBeFinished(ctx context.Context, cfg *Cfg) (ready bool, err error) {
	// If no execution client is set, then we can skip this step
	if cfg.executionClient == nil {
		return true, nil
	}

	// Setup the timers
	readyTimeout := time.NewTimer(10 * time.Second)
	readyInterval := time.NewTimer(50 * time.Millisecond)

	// Ensure the timers are stopped to release resources
	defer readyTimeout.Stop()
	defer readyInterval.Stop()

	// Loop to check the readiness status
	for {
		select {
		case <-ctx.Done():
			// Context canceled or timed out
			return false, ctx.Err()
		case <-readyTimeout.C:
			// Timeout reached without the execution engine being ready
			return false, nil
		case <-readyInterval.C:
			// Check the readiness of the execution engine
			ready, err := cfg.executionClient.Ready(ctx)
			if err != nil {
				return false, err
			}
			if !ready {
				// If not ready, continue checking in the next interval
				continue
			}
			// Execution engine is ready
			return true, nil
		}
	}
}

// fetchBlocksFromReqResp retrieves blocks starting from a specified block number and continues for a given count.
// It sends a request to fetch the blocks, verifies the associated blobs, and inserts them into the blob store.
// It returns a PeeredObject containing the blocks and the peer ID, or an error if something goes wrong.
func fetchBlocksFromReqResp(ctx context.Context, cfg *Cfg, from uint64, count uint64) (*peers.PeeredObject[[]*cltypes.SignedBeaconBlock], error) {
	// spam requests to fetch blocks by range from the execution client
	blocks, pid, err := cfg.rpc.SendBeaconBlocksByRangeReq(ctx, from, count)
	for err != nil {
		blocks, pid, err = cfg.rpc.SendBeaconBlocksByRangeReq(ctx, from, count)
	}

	// If no blocks are returned, return nil without error
	if len(blocks) == 0 {
		return nil, nil
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Block.Slot < blocks[j].Block.Slot
	})

	denebBlocks := []*cltypes.SignedBeaconBlock{}
	fuluBlocks := []*cltypes.SignedBlindedBeaconBlock{}
	for _, block := range blocks {
		blindedBlock, err := block.Blinded()
		if err != nil {
			return nil, err
		}
		if block.Version() >= clparams.FuluVersion {
			fuluBlocks = append(fuluBlocks, blindedBlock)
		} else if block.Version() >= clparams.DenebVersion {
			denebBlocks = append(denebBlocks, block)
		}
	}

	if len(fuluBlocks) > 0 {
		// download missing column data for the fulu blocks
		if cfg.caplinConfig.ArchiveBlobs || cfg.caplinConfig.ImmediateBlobsBackfilling {
			if err := cfg.peerDas.DownloadColumnsAndRecoverBlobs(ctx, fuluBlocks); err != nil {
				log.Warn("[chainTipSync] failed to download columns and recover blobs", "err", err)
			}
		} else {
			if err := cfg.peerDas.DownloadOnlyCustodyColumns(ctx, fuluBlocks); err != nil {
				log.Warn("[chainTipSync] failed to download only custody columns", "err", err)
			}
		}
	}

	if len(denebBlocks) > 0 {
		// Generate blob identifiers from the retrieved blocks
		ids, err := network2.BlobsIdentifiersFromBlocks(denebBlocks, cfg.beaconCfg)
		if err != nil {
			return nil, err
		}
		var inserted uint64
		// Loop until all blobs are inserted into the blob store
		for inserted != uint64(ids.Len()) {
			select {
			case <-ctx.Done():
				// Context canceled or timed out
				return nil, ctx.Err()
			default:
			}

			// Request blobs frantically from the execution client
			blobs, err := network2.RequestBlobsFrantically(ctx, cfg.rpc, ids)
			if err != nil {
				return nil, fmt.Errorf("failed to request blobs frantically: %w", err)
			}

			// Verify the blobs against identifiers and insert them into the blob store
			if _, inserted, err = blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStore, ids, blobs.Responses, nil); err != nil {
				return nil, fmt.Errorf("failed to verify blobs against identifiers and insert into the blob store: %w", err)
			}
		}
	}

	// Return the blocks and the peer ID wrapped in a PeeredObject
	return &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{
		Data: blocks,
		Peer: pid,
	}, nil
}

// startFetchingBlocksMissedByGossipAfterSomeTime starts fetching blocks that might have been missed by gossip after a delay.
// It periodically fetches blocks from the highest seen block up to the current slot and sends the results or errors to the provided channels.
func startFetchingBlocksMissedByGossipAfterSomeTime(ctx context.Context, cfg *Cfg, args Args, respCh chan<- *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], errCh chan error) {
	// Wait for half the duration of SecondsPerSlot or until the context is done
	select {
	case <-time.After((time.Duration(cfg.beaconCfg.SecondsPerSlot) * time.Second) / 2):
	case <-ctx.Done():
		return
	}

	// Continuously fetch and process blocks
	for {
		// Calculate the range of blocks to fetch
		from := cfg.forkChoice.HighestSeen() - 2
		currentSlot := cfg.ethClock.GetCurrentSlot()
		count := (currentSlot - from) + 4

		// Stop fetching if the highest seen block is greater than or equal to the target slot
		if cfg.forkChoice.HighestSeen() >= args.targetSlot {
			return
		}

		// Fetch blocks from the specified range
		blocks, err := fetchBlocksFromReqResp(ctx, cfg, from, count)
		if err != nil {
			// Send error to the error channel and return
			errCh <- err
			return
		}

		// Send fetched blocks to the response channel or handle context cancellation
		select {
		case respCh <- blocks:
		case <-ctx.Done():
			return
		case <-time.After(time.Second): // Take a short pause before the next iteration
		}
	}
}

// listenToIncomingBlocksUntilANewBlockIsReceived listens for incoming blocks until a new block with a slot greater than or equal to the target slot is received.
// It processes blocks, checks their validity, and publishes them. It also handles context cancellation and logs progress periodically.
func listenToIncomingBlocksUntilANewBlockIsReceived(ctx context.Context, logger log.Logger, cfg *Cfg, args Args, respCh <-chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], errCh chan error) error {
	// Timer to log progress every 30 seconds
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	// Timer to check block presence every 20 milliseconds
	presenceTicker := time.NewTicker(20 * time.Millisecond)
	defer presenceTicker.Stop()

	// Map to keep track of seen block roots
	seenBlockRoots := make(map[common.Hash]struct{})
MainLoop:
	for {
		select {
		case <-presenceTicker.C:
			// Check if the highest seen block is greater than or equal to the target slot
			if cfg.forkChoice.HighestSeen() >= args.targetSlot {
				break MainLoop
			}
		case <-ctx.Done():
			// Handle context cancellation
			return ctx.Err()
		case err := <-errCh:
			// Handle errors received on the error channel
			return err
		case blocks := <-respCh:
			// Handle blocks received on the response channel
			for _, block := range blocks.Data {
				// Check if the parent block is known
				if _, ok := cfg.forkChoice.GetHeader(block.Block.ParentRoot); !ok {
					time.Sleep(time.Millisecond)
					continue
				}

				// Calculate the block root and check if the block is already known
				blockRoot, _ := block.Block.HashSSZ() // Ignoring error as block would not process if HashSSZ failed
				if _, ok := cfg.forkChoice.GetHeader(blockRoot); ok {
					// Check if the block slot is greater than or equal to the target slot
					if block.Block.Slot >= args.targetSlot {
						break MainLoop
					}
					continue
				}

				// Check if the block root has already been seen
				if _, ok := seenBlockRoots[blockRoot]; ok {
					continue
				}

				// Process the block
				if err := processBlock(ctx, cfg, cfg.indiciesDB, block, true, true, true); err != nil {
					log.Debug("bad blocks segment received", "err", err, "blockSlot", block.Block.Slot)
					continue
				}

				// Mark the block root as seen
				seenBlockRoots[blockRoot] = struct{}{}

				// Check if the block slot is greater than or equal to the target slot
				if block.Block.Slot >= args.targetSlot {
					break MainLoop
				}
			}
		case <-logTicker.C:
			// Log progress periodically
			logger.Info("[Caplin] Progress", "progress", cfg.forkChoice.HighestSeen(), "from", args.seenSlot, "to", args.targetSlot)
		}
	}
	return nil
}

// chainTipSync synchronizes the chain tip by fetching blocks from the highest seen block up to the target slot by listening to incoming blocks.
// or by fetching blocks that might have been missed by gossip after a delay.
func chainTipSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	totalRequest := args.targetSlot - args.seenSlot
	log.Debug("[chainTipSync] totalRequest", "totalRequest", totalRequest, "seenSlot", args.seenSlot, "targetSlot", args.targetSlot)
	// If the execution engine is not ready, wait for it to be ready.
	ready, err := waitForExecutionEngineToBeFinished(ctx, cfg)
	if err != nil {
		log.Warn("[chainTipSync] error waiting for execution engine to be ready", "err", err)
		return err
	}
	if !ready {
		log.Debug("[chainTipSync] execution engine is not ready yet")
		return nil
	}

	log.Debug("[chainTipSync] execution engine is ready")

	if cfg.executionClient != nil && cfg.executionClient.SupportInsertion() {
		if err := cfg.blockCollector.Flush(context.Background()); err != nil {
			return err
		}
	}

	logger.Debug("waiting for blocks...",
		"seenSlot", args.seenSlot,
		"targetSlot", args.targetSlot,
		"requestedSlots", totalRequest,
	)
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1024)
	errCh := make(chan error)

	// 25 seconds is a good timeout for this
	ctx, cn := context.WithTimeout(ctx, 25*time.Second)
	defer cn()

	go startFetchingBlocksMissedByGossipAfterSomeTime(ctx, cfg, args, respCh, errCh)

	return listenToIncomingBlocksUntilANewBlockIsReceived(ctx, logger, cfg, args, respCh, errCh)
}
