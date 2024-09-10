package freezeblocks

import (
	"context"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
)

func Sqeeze(ctx context.Context, dirs datadir.Dirs, from, to string, logger log.Logger) error {
	logger.Info("[recompress] file", "f", to)
	decompressor, err := seg.NewDecompressor(from)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.EnableReadAhead().DisableReadAhead()
	r := seg.NewReader(decompressor.MakeGetter(), seg.DetectCompressType(decompressor.MakeGetter()))

	compressCfg := BlockCompressCfg
	compressCfg.Workers = estimate.CompressSnapshot.Workers()
	c, err := seg.NewCompressor(ctx, "recompress", to, dirs.Tmp, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	w := seg.NewWriter(c, seg.CompressKeys|seg.CompressVals)
	var k, v []byte
	var i int
	for r.HasNext() {
		i++
		k, _ = r.Next(k[:0])
		v, _ = r.Next(v[:0])
		if err = w.AddWord(k); err != nil {
			return err
		}
		if err = w.AddWord(v); err != nil {
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
