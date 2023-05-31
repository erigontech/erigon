package borfinality

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/log/v3"
)

type config struct {
	engine     consensus.Engine
	stagedSync *stagedsync.Sync
	db         kv.RwDB
	logger     log.Logger
	borAPI     BorAPI
	closeCh    chan struct{}
}

func Whitelist(engine consensus.Engine, stagedsync *stagedsync.Sync, db kv.RwDB, logger log.Logger, borAPI BorAPI, closeCh chan struct{}) {
	config := &config{
		engine:     engine,
		stagedSync: stagedsync,
		db:         db,
		logger:     logger,
		borAPI:     borAPI,
		closeCh:    closeCh,
	}

	go startCheckpointWhitelistService(config)
	go startMilestoneWhitelistService(config)
	go startNoAckMilestoneService(config)
	go startNoAckMilestoneByIDService(config)
}

var (
	ErrNotBorConsensus             = errors.New("not bor consensus was given")
	ErrBorConsensusWithoutHeimdall = errors.New("bor consensus without heimdall")
)

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

func RetryHeimdallHandler(fn heimdallHandler, config *config, tickerDuration time.Duration, timeout time.Duration, fnName string) {
	retryHeimdallHandler(fn, config, tickerDuration, timeout, fnName, getBorHandler)
}

func retryHeimdallHandler(fn heimdallHandler, config *config, tickerDuration time.Duration, timeout time.Duration, fnName string, getBorHandler func(chain *config) (*bor.Bor, error)) {
	// a shortcut helps with tests and early exit
	select {
	case <-config.closeCh:
		return
	default:
	}

	bor, err := getBorHandler(config)
	if err != nil {
		log.Error("error while getting the borHandler", "err", err)
		return
	}

	// first run for fetching milestones
	firstCtx, cancel := context.WithTimeout(context.Background(), timeout)
	err = fn(firstCtx, bor, config)

	cancel()

	if err != nil {
		log.Warn(fmt.Sprintf("unable to start the %s service - first run", fnName), "err", err)
	}

	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := fn(ctx, bor, config)

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
func handleWhitelistCheckpoint(ctx context.Context, bor *bor.Bor, config *config) error {
	// Create a new bor verifier, which will be used to verify checkpoints and milestones
	verifier := newBorVerifier()
	service := whitelist.GetWhitelistingService()

	blockNum, blockHash, err := fetchWhitelistCheckpoint(ctx, bor, verifier, config)
	// If the array is empty, we're bound to receive an error. Non-nill error and non-empty array
	// means that array has partial elements and it failed for some block. We'll add those partial
	// elements anyway.
	if err != nil {
		return err
	}

	service.ProcessCheckpoint(blockNum, blockHash)

	return nil
}

type heimdallHandler func(ctx context.Context, bor *bor.Bor, config *config) error

// handleMilestone handles the milestone mechanism.
func handleMilestone(ctx context.Context, bor *bor.Bor, config *config) error {
	// Create a new bor verifier, which will be used to verify checkpoints and milestones
	verifier := newBorVerifier()
	num, hash, err := fetchWhitelistMilestone(ctx, bor, verifier, config)

	service := whitelist.GetWhitelistingService()

	// If the current chain head is behind the received milestone, add it to the future milestone
	// list. Also, the hash mismatch (end block hash) error will lead to rewind so also
	// add that milestone to the future milestone list.
	if errors.Is(err, errMissingBlocks) || errors.Is(err, errHashMismatch) {
		service.ProcessFutureMilestone(num, hash)
	}

	if err != nil {
		return err
	}

	service.ProcessMilestone(num, hash)

	return nil
}

func handleNoAckMilestone(ctx context.Context, bor *bor.Bor, config *config) error {
	milestoneID, err := fetchNoAckMilestone(ctx, bor)
	service := whitelist.GetWhitelistingService()

	//If failed to fetch the no-ack milestone then it give the error.
	if err != nil {
		return err
	}

	service.RemoveMilestoneID(milestoneID)

	return nil
}

func handleNoAckMilestoneByID(ctx context.Context, bor *bor.Bor, config *config) error {
	service := whitelist.GetWhitelistingService()
	milestoneIDs := service.GetMilestoneIDsList()

	for _, milestoneID := range milestoneIDs {
		// todo: check if we can ignore the error
		err := fetchNoAckMilestoneByID(ctx, bor, milestoneID)
		if err == nil {
			service.RemoveMilestoneID(milestoneID)
		}
	}

	return nil
}

func getBorHandler(config *config) (*bor.Bor, error) {

	bor, ok := config.engine.(*bor.Bor)
	if !ok {
		return nil, ErrNotBorConsensus
	}

	if bor.HeimdallClient == nil {
		return nil, ErrBorConsensusWithoutHeimdall
	}

	return bor, nil
}
