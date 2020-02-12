package common

import (
	"context"
)

func IsCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		// nothing to do
	}

	return false
}
