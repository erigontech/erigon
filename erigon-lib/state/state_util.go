package state

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"
)

func buildSimpleMapAccessor(ctx context.Context, d *seg.Decompressor, compression FileCompression, cfg recsplit.RecSplitArgs, logger log.Logger, walker func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error) error {
	count := d.Count()

	defer d.EnableReadAhead().DisableReadAhead()

	var rs *recsplit.RecSplit
	var err error
	cfg.KeyCount = count
	if rs, err = recsplit.NewRecSplit(cfg, logger); err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	for {
		g := NewArchiveGetter(d.MakeGetter(), compression)
		var i, offset, nextPos uint64
		word := make([]byte, 0, 256)
		for g.HasNext() {
			word, nextPos = g.Next(word[:0])
			if err := walker(rs, i, offset, word); err != nil {
				return err
			}
			i++
			offset = nextPos

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		if err = rs.Build(ctx); err != nil {
			if rs.Collision() {
				logger.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	return nil
}
