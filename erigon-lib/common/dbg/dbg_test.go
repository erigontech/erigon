package dbg

import (
	"context"
	"fmt"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
)

func exec() string {
	return "hola"
}
func TestLog(t *testing.T) {
	ctx := ContextWithDebug(context.Background(), true)

	prefixFn := func() string {
		return fmt.Sprintf("[dbg] BlockReader(idxMax=%d,segMax=%d).BodyWithTransactions(hash=%x,blk=%d) -> ", 1, 2, 3, 4)
	}

	DbgLog(ctx, log.LvlWarn, prefixFn).Msg("got nil body from file").Entry("exec_value", func() string {
		return fmt.Sprintf("eval_exec %s", exec())
	}).Log()

	DbgLog(ctx, log.LvlWarn, prefixFn).Log()
}
