package utils

import (
	"context"
	"time"
)

func Sleep(parentContext context.Context, timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(parentContext, timeout)
	defer cancel()
	<-ctx.Done()
}
