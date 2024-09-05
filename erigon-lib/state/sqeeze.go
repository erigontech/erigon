package state

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
)

// Sqeeze - re-compress file with care of ForeignKeys
func (a *Aggregator) Sqeeze(ctx context.Context, domain kv.Domain, from, to string) error {
	a.logger.Info("[recompress] file", "f", to)
	decompressor, err := seg.NewDecompressor(from)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.EnableReadAhead().DisableReadAhead()
	r := seg.NewReader(decompressor.MakeGetter(), seg.DetectCompressType(decompressor.MakeGetter()))

	c, err := seg.NewCompressor(ctx, "recompress", to, a.dirs.Tmp, a.d[domain].compressCfg, log.LvlInfo, a.logger)
	if err != nil {
		return err
	}
	defer c.Close()
	w := seg.NewWriter(c, seg.CompressKeys)
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
