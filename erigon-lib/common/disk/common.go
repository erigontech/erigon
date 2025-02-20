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

package disk

import (
	"context"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
)

func UpdateDiskStats(ctx context.Context, logger log.Logger) {
	logEvery := time.NewTicker(5 * time.Second)
	defer logEvery.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:

			if err := UpdatePrometheusDiskStats(); err != nil {
				logger.Warn("[disk] error disk fault stats", "err", err)
			}
		}
	}
}
