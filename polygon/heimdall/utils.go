package heimdall

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

type headerReader interface {
	HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error)
}

func CalculateEventWindow(ctx context.Context, config *borcfg.BorConfig, header *types.Header, tx kv.Getter, headerReader headerReader) (from time.Time, to time.Time, err error) {

	blockNum := header.Number.Uint64()
	blockNum += blockNum % config.CalculateSprintLength(blockNum)

	prevHeader, err := headerReader.HeaderByNumber(ctx, tx, blockNum-config.CalculateSprintLength(blockNum))

	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("window calculation failed: %w", err)
	}

	if config.IsIndore(blockNum) {
		stateSyncDelay := config.CalculateStateSyncDelay(blockNum)
		to = time.Unix(int64(header.Time-stateSyncDelay), 0)
		from = time.Unix(int64(prevHeader.Time-stateSyncDelay), 0)
	} else {
		to = time.Unix(int64(prevHeader.Time), 0)
		prevHeader, err := headerReader.HeaderByNumber(ctx, tx, prevHeader.Number.Uint64()-config.CalculateSprintLength(prevHeader.Number.Uint64()))

		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("window calculation failed: %w", err)
		}

		from = time.Unix(int64(prevHeader.Time), 0)
	}

	return from, to, nil
}
