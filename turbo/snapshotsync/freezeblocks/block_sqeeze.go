package freezeblocks

import (
	"context"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
)

func Sqeeze(ctx context.Context, dirs datadir.Dirs, from, to string, logger log.Logger) error {
	logger.Info("[sqeeze] file", "f", to)
	decompressor, err := seg.NewDecompressor(from)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.EnableReadAhead().DisableReadAhead()
	g := decompressor.MakeGetter()

	compressCfg := BlockCompressCfg
	compressCfg.Workers = estimate.CompressSnapshot.Workers()
	c, err := seg.NewCompressor(ctx, "sqeeze", to, dirs.Tmp, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	var k []byte
	var i int
	for g.HasNext() {
		i++
		k, _ = g.Next(k[:0])
		if err = c.AddWord(k); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := c.Compress(); err != nil {
		return err
	}

	return nil
}
