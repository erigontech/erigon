package common

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/debug"
)

func IsCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		fmt.Println("============================= Context.Cancel", debug.Callers(20))
		return true
	default:
		// nothing to do
	}

	return false
}
