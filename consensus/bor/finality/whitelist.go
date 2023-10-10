package finality

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/flags"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/whitelist"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type config struct {
	heimdall    heimdall.IHeimdallClient
	borDB       kv.RwDB
	chainDB     kv.RwDB
	blockReader services.BlockReader
	logger      log.Logger
	borAPI      BorAPI
	closeCh     chan struct{}
}

type BorAPI interface {
	GetRootHash(start uint64, end uint64) (string, error)
}

func Whitelist(heimdall heimdall.IHeimdallClient, borDB kv.RwDB, chainDB kv.RwDB, blockReader services.BlockReader, logger log.Logger, borAPI BorAPI, closeCh chan struct{}) {
	if !flags.Milestone {
		return
	}

	config := &config{
		heimdall:    heimdall,
		borDB:       borDB,
		chainDB:     chainDB,
		blockReader: blockReader,
		logger:      logger,
		borAPI:      borAPI,
		closeCh:     closeCh,
	}

	go startCheckpointWhitelistService(config)
	go startMilestoneWhitelistService(config)
	go startNoAckMilestoneService(config)
	go startNoAckMilestoneByIDService(config)
}

const (
	whitelistTimeout      = 30 * time.Second
	noAckMilestoneTimeout = 4 * time.Second
)

// StartCheckpointWhitelistService starts the goroutine to fetch checkpoints and update the
// checkpoint whitelist map.
func startCheckpointWhitelistService(config *config) {
	const (
		tickerDuration = 100 * time.Second
		fnName         = "whitelist checkpoint"
	)

	RetryHeimdallHandler(handleWhitelistCheckpoint, config, tickerDuration, whitelistTimeout, fnName)
}

// startMilestoneWhitelistService starts the goroutine to fetch milestiones and update the
// milestone whitelist map.
func startMilestoneWhitelistService(config *config) {
	const (
		tickerDuration = 12 * time.Second
		fnName         = "whitelist milestone"
	)

	RetryHeimdallHandler(handleMilestone, config, tickerDuration, whitelistTimeout, fnName)
}

func startNoAckMilestoneService(config *config) {
	const (
		tickerDuration = 6 * time.Second
		fnName         = "no-ack-milestone service"
	)

	RetryHeimdallHandler(handleNoAckMilestone, config, tickerDuration, noAckMilestoneTimeout, fnName)
}

func startNoAckMilestoneByIDService(config *config) {
	const (
		tickerDuration = 1 * time.Minute
		fnName         = "no-ack-milestone-by-id service"
	)

	RetryHeimdallHandler(handleNoAckMilestoneByID, config, tickerDuration, noAckMilestoneTimeout, fnName)
}

type heimdallHandler func(ctx context.Context, heimdallClient heimdall.IHeimdallClient, config *config) error

func RetryHeimdallHandler(fn heimdallHandler, config *config, tickerDuration time.Duration, timeout time.Duration, fnName string) {
	retryHeimdallHandler(fn, config, tickerDuration, timeout, fnName)
}

func retryHeimdallHandler(fn heimdallHandler, config *config, tickerDuration time.Duration, timeout time.Duration, fnName string) {
	// a shortcut helps with tests and early exit
	select {
	case <-config.closeCh:
		return
	default:
	}

	if config.heimdall == nil {
		log.Error("bor engine not available")
		return
	}

	// first run for fetching milestones
	firstCtx, cancel := context.WithTimeout(context.Background(), timeout)
	err := fn(firstCtx, config.heimdall, config)

	cancel()

	if err != nil {
		log.Warn(fmt.Sprintf("unable to start the %s service - first run", fnName), "err", err)
	}

	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	for {
		defer func() {
			r := recover()
			if r != nil {
				log.Warn(fmt.Sprintf("service %s- run failed with panic", fnName), "err", r)
			}
		}()

		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := fn(ctx, config.heimdall, config)

			cancel()

			if err != nil {
				log.Warn(fmt.Sprintf("unable to handle %s", fnName), "err", err)
			}
		case <-config.closeCh:
			return
		}
	}
}

// handleWhitelistCheckpoint handles the checkpoint whitelist mechanism.
func handleWhitelistCheckpoint(ctx context.Context, heimdallClient heimdall.IHeimdallClient, config *config) error {
	service := whitelist.GetWhitelistingService()

	// Create a new bor verifier, which will be used to verify checkpoints and milestones
	verifier := newBorVerifier()
	blockNum, blockHash, err := fetchWhitelistCheckpoint(ctx, heimdallClient, verifier, config)

	// If the array is empty, we're bound to receive an error. Non-nill error and non-empty array
	// means that array has partial elements and it failed for some block. We'll add those partial
	// elements anyway.
	if err != nil {
		return err
	}

	service.ProcessCheckpoint(blockNum, blockHash)

	return nil
}

// handleMilestone handles the milestone mechanism.
func handleMilestone(ctx context.Context, heimdallClient heimdall.IHeimdallClient, config *config) error {
	service := whitelist.GetWhitelistingService()

	// Create a new bor verifier, which will be used to verify checkpoints and milestones
	verifier := newBorVerifier()
	num, hash, err := fetchWhitelistMilestone(ctx, heimdallClient, verifier, config)

	// If the current chain head is behind the received milestone, add it to the future milestone
	// list. Also, the hash mismatch (end block hash) error will lead to rewind so also
	// add that milestone to the future milestone list.
	if errors.Is(err, errMissingBlocks) || errors.Is(err, errHashMismatch) {
		service.ProcessFutureMilestone(num, hash)
		return nil
	}

	if errors.Is(err, heimdall.ErrServiceUnavailable) {
		return nil
	}

	if err != nil {
		return err
	}

	service.ProcessMilestone(num, hash)

	return nil
}

func handleNoAckMilestone(ctx context.Context, heimdallClient heimdall.IHeimdallClient, config *config) error {
	service := whitelist.GetWhitelistingService()
	milestoneID, err := fetchNoAckMilestone(ctx, heimdallClient, config.logger)

	if errors.Is(err, heimdall.ErrServiceUnavailable) {
		return nil
	}

	if err != nil {
		return err
	}

	service.RemoveMilestoneID(milestoneID)

	return nil
}

func handleNoAckMilestoneByID(ctx context.Context, heimdallClient heimdall.IHeimdallClient, config *config) error {
	service := whitelist.GetWhitelistingService()
	milestoneIDs := service.GetMilestoneIDsList()

	for _, milestoneID := range milestoneIDs {
		err := fetchNoAckMilestoneByID(ctx, heimdallClient, milestoneID, config.logger)
		if err == nil {
			service.RemoveMilestoneID(milestoneID)
		}
	}

	return nil
}
