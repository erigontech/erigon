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

package utils

import (
	"context"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"

	libcommon "github.com/erigontech/erigon-lib/common"
)

func Retry(
	ctx context.Context,
	retryCount int,
	delayForAttempt func(attempt int) time.Duration,
	isRecoverableError func(error) bool,
	logger log.Logger,
	opName string,
	op func(context.Context) (interface{}, error),
) (interface{}, error) {
	var result interface{}
	var err error

	for i := 0; i <= retryCount; i++ {
		if i > 0 {
			logger.Trace("retrying", "op", opName, "attempt", i, "err", err)
			if err := libcommon.Sleep(ctx, delayForAttempt(i)); err != nil {
				return nil, err
			}
		}
		result, err = op(ctx)
		if (err == nil) || !isRecoverableError(err) {
			break
		}
	}
	return result, err
}
