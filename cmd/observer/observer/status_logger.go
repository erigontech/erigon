package observer

import (
	"context"
	"errors"
	"time"

	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/cmd/observer/utils"
	"github.com/ledgerwatch/log/v3"
)

func StatusLoggerLoop(ctx context.Context, db database.DB, networkID uint, period time.Duration, logger log.Logger) {
	var maxPingTries uint = 1000000 // unlimited (include dead nodes)
	var prevTotalCount uint
	var prevDistinctIPCount uint

	for ctx.Err() == nil {
		utils.Sleep(ctx, period)

		totalCount, err := db.CountNodes(ctx, maxPingTries, networkID)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error("Failed to count nodes", "err", err)
			}
			continue
		}

		distinctIPCount, err := db.CountIPs(ctx, maxPingTries, networkID)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error("Failed to count IPs", "err", err)
			}
			continue
		}

		if (totalCount == prevTotalCount) && (distinctIPCount == prevDistinctIPCount) {
			continue
		}

		logger.Info("Status", "totalCount", totalCount, "distinctIPCount", distinctIPCount)
		prevTotalCount = totalCount
		prevDistinctIPCount = distinctIPCount
	}
}
