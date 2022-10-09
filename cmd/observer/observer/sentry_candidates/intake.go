package sentry_candidates

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/cmd/observer/observer/node_utils"
	"github.com/ledgerwatch/erigon/cmd/observer/utils"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
	"github.com/nxadm/tail"
)

type Intake struct {
	logPath   string
	db        database.DBRetrier
	saveQueue *utils.TaskQueue
	chain     string

	handshakeRefreshTimeout time.Duration

	statusLogPeriod time.Duration
	log             log.Logger
}

func NewIntake(
	logPath string,
	db database.DBRetrier,
	saveQueue *utils.TaskQueue,
	chain string,
	handshakeRefreshTimeout time.Duration,
	statusLogPeriod time.Duration,
	logger log.Logger,
) *Intake {
	instance := Intake{
		logPath,
		db,
		saveQueue,
		chain,
		handshakeRefreshTimeout,
		statusLogPeriod,
		logger,
	}
	return &instance
}

func (intake *Intake) Run(ctx context.Context) error {
	tailConfig := tail.Config{
		MustExist:     true,
		Follow:        true,
		CompleteLines: true,
	}
	logFile, err := tail.TailFile(intake.logPath, tailConfig)
	if err != nil {
		return err
	}
	defer func() {
		_ = logFile.Stop()
		logFile.Cleanup()
	}()
	eventLog := NewLog(NewTailLineReader(ctx, logFile))

	var lastEventTime *time.Time
	lastEventTime, err = intake.db.FindSentryCandidatesLastEventTime(ctx)
	if err != nil {
		return err
	}

	doneCount := 0
	statusLogDate := time.Now()

	for {
		event, err := eventLog.Read()
		if err != nil {
			return err
		}
		if event == nil {
			break
		}

		if (event.NodeURL == "") || (event.ClientID == "") {
			continue
		}

		// Skip all events processed previously.
		// The time is stored with a second precision, hence adding a slack.
		if (lastEventTime != nil) && !event.Timestamp.After(lastEventTime.Add(time.Second)) {
			continue
		}

		doneCount++
		if time.Since(statusLogDate) > intake.statusLogPeriod {
			intake.log.Info(
				"Sentry candidates intake",
				"done", doneCount,
			)
			statusLogDate = time.Now()
		}

		peerNode, err := enode.ParseV4(event.NodeURL)
		if err != nil {
			return err
		}

		networkID := params.NetworkIDByChainName(intake.chain)
		isCompatFork := true

		handshakeRetryTime := time.Now().Add(intake.handshakeRefreshTimeout)
		crawlRetryTime := time.Now()

		intake.log.Trace("sentry_candidates.Intake saving peer",
			"timestamp", event.Timestamp,
			"peerNode", peerNode,
			"clientID", event.ClientID,
		)

		intake.saveQueue.EnqueueTask(ctx, func(ctx context.Context) error {
			return intake.savePeer(
				ctx,
				event.Timestamp,
				peerNode,
				event.ClientID,
				networkID,
				isCompatFork,
				event.EthVersion(),
				handshakeRetryTime,
				crawlRetryTime)
		})
	}
	return nil
}

func (intake *Intake) savePeer(
	ctx context.Context,
	eventTime time.Time,
	peer *enode.Node,
	clientID string,
	networkID uint64,
	isCompatFork bool,
	ethVersion uint,
	handshakeRetryTime time.Time,
	crawlRetryTime time.Time,
) error {
	id, err := node_utils.NodeID(peer)
	if err != nil {
		return fmt.Errorf("failed to get peer node ID: %w", err)
	}

	var dbErr error

	// Update the eventTime early. If the save fails, the candidate will be skipped on the next run.
	dbErr = intake.db.UpdateSentryCandidatesLastEventTime(ctx, eventTime)
	if dbErr != nil {
		return dbErr
	}

	dbErr = intake.db.UpsertNodeAddr(ctx, id, node_utils.MakeNodeAddr(peer))
	if dbErr != nil {
		return dbErr
	}

	dbErr = intake.db.ResetPingError(ctx, id)
	if dbErr != nil {
		return dbErr
	}

	dbErr = intake.db.UpdateClientID(ctx, id, clientID)
	if dbErr != nil {
		return dbErr
	}

	dbErr = intake.db.UpdateNetworkID(ctx, id, uint(networkID))
	if dbErr != nil {
		return dbErr
	}

	dbErr = intake.db.UpdateForkCompatibility(ctx, id, isCompatFork)
	if dbErr != nil {
		return dbErr
	}

	if ethVersion != 0 {
		dbErr = intake.db.UpdateEthVersion(ctx, id, ethVersion)
		if dbErr != nil {
			return dbErr
		}
	}

	dbErr = intake.db.DeleteHandshakeErrors(ctx, id)
	if dbErr != nil {
		return dbErr
	}

	dbErr = intake.db.UpdateHandshakeTransientError(ctx, id, false)
	if dbErr != nil {
		return dbErr
	}

	dbErr = intake.db.UpdateHandshakeRetryTime(ctx, id, handshakeRetryTime)
	if dbErr != nil {
		return dbErr
	}

	return intake.db.UpdateCrawlRetryTime(ctx, id, crawlRetryTime)
}
