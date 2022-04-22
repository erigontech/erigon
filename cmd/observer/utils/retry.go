package utils

import (
	"context"
	"github.com/ledgerwatch/log/v3"
	"time"
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
			Sleep(ctx, delayForAttempt(i))
		}
		result, err = op(ctx)
		if (err == nil) || !isRecoverableError(err) {
			break
		}
	}
	return result, err
}
