package common

import (
	"context"
	"time"
)

// Sleep which is context aware. Blocks for d duration and returns no error, unless the context
// has been cancelled, timed out, deadline reached, etc. in which case it returns ctx.Err().
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
