package utils

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
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
