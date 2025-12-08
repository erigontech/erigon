package freezeblocks

import (
	"context"

	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
)

func Sqeeze(ctx context.Context, dirs datadir.Dirs, from, to string, logger log.Logger) error {
	logger.Info("[sqeeze] file", "f", to)
	decompressor, err := seg.NewDecompressor(from)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.MadvSequential().DisableReadAhead()
	g := decompressor.MakeGetter()

	compressCfg := BlockCompressCfg
	compressCfg.Workers = estimate.CompressSnapshot.Workers()
	c, err := seg.NewCompressor(ctx, "sqeeze", to, dirs.Tmp, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.ReadFrom(g); err != nil {
		return err
	}
	if err := c.Compress(); err != nil {
		return err
	}

	return nil
}
