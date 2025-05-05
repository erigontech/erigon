// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package observer

import (
	"context"
	"errors"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cmd/observer/database"
)

func StatusLoggerLoop(ctx context.Context, db database.DB, networkID uint, period time.Duration, logger log.Logger) {
	var maxPingTries uint = 1000000 // unlimited (include dead nodes)
	var prevTotalCount uint
	var prevDistinctIPCount uint

	for ctx.Err() == nil {
		if err := common.Sleep(ctx, period); err != nil {
			break
		}

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
