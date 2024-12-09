package engine_logs_pammer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/log/v3"
)

type EngineLogsSpammer struct {
	lastRequestTime atomic.Value
	logger          log.Logger
	chainConfig     *chain.Config
}

func NewEngineLogsSpammer(logger log.Logger, chainConfig *chain.Config) *EngineLogsSpammer {
	lastRequestTimeAtomic := atomic.Value{}
	lastRequestTimeAtomic.Store(time.Now())
	return &EngineLogsSpammer{
		logger:          logger,
		chainConfig:     chainConfig,
		lastRequestTime: lastRequestTimeAtomic,
	}
}

func (e *EngineLogsSpammer) Start(ctx context.Context) {
	e.lastRequestTime.Store(time.Now())
	logSpamInterval := 15 * time.Second
	offlineTimeThreshold := 30 * time.Second
	if !e.chainConfig.TerminalTotalDifficultyPassed {
		return
	}
	go func() {
		intervalSpam := time.NewTicker(logSpamInterval)
		defer intervalSpam.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-intervalSpam.C:
				if time.Since(e.lastRequestTime.Load().(time.Time)) > offlineTimeThreshold {
					e.logger.Warn("flag --externalcl was provided, but no CL seems to be connected.")
				}
			}
		}
	}()
}

func (e *EngineLogsSpammer) RecordRequest() {
	e.lastRequestTime.Store(time.Now())
}
