package engine_logs_spammer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/chain"
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
	logSpamInterval := 60 * time.Second
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
				ts := time.Since(e.lastRequestTime.Load().(time.Time)).Round(1 * time.Second)
				if ts > logSpamInterval {
					e.logger.Warn("flag --externalcl was provided, but no CL requests to engine-api in " + ts.String())
				}
			}
		}
	}()
}

func (e *EngineLogsSpammer) RecordRequest() {
	e.lastRequestTime.Store(time.Now())
}
