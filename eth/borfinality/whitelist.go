package borfinality

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/log/v3"
)

type BorAPI interface {
	GetRootHash(start uint64, end uint64) (string, error)
}

type config struct {
	engine  consensus.Engine
	borDB   kv.RwDB
	chainDB kv.RwDB
	logger  log.Logger
	borAPI  BorAPI
	closeCh chan struct{}
}

func Whitelist(engine consensus.Engine, borDB kv.RwDB, chainDB kv.RwDB, logger log.Logger, borAPI BorAPI, closeCh chan struct{}) {
	config := &config{
		engine:  engine,
		borDB:   borDB,
		chainDB: chainDB,
		logger:  logger,
		borAPI:  borAPI,
		closeCh: closeCh,
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

type heimdallHandler func(ctx context.Context, bor *bor.Bor, config *config) error

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

	bor, ok := config.engine.(*bor.Bor)
	if !ok {
		log.Error("bor engine not available")
	}

	// first run for fetching milestones
	firstCtx, cancel := context.WithTimeout(context.Background(), timeout)
	err := fn(firstCtx, bor, config)

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
