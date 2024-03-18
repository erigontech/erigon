package bortype

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	borTypes := append(snaptype.BlockSnapshotTypes, BorSnapshotTypes...)

	snapcfg.RegisterKnownTypes(networkname.MumbaiChainName, borTypes)
	snapcfg.RegisterKnownTypes(networkname.AmoyChainName, borTypes)
	snapcfg.RegisterKnownTypes(networkname.BorMainnetChainName, borTypes)
}

var (
	BorEvents = snaptype.RegisterType(
		snaptype.Enums.BorEvents,
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		[]snaptype.Index{snaptype.Indexes.BorTxnHash},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, sn snaptype.FileInfo, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						err = fmt.Errorf("BorEventsIdx: at=%d-%d, %v, %s", sn.From, sn.To, rec, dbg.Stack())
					}
				}()
				// Calculate how many records there will be in the index
				d, err := seg.NewDecompressor(sn.Path)
				if err != nil {
					return err
				}
				defer d.Close()
				g := d.MakeGetter()
				var blockNumBuf [length.BlockNum]byte
				var first bool = true
				word := make([]byte, 0, 4096)
				var blockCount int
				var baseEventId uint64
				for g.HasNext() {
					word, _ = g.Next(word[:0])
					if first || !bytes.Equal(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum]) {
						blockCount++
						copy(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum])
					}
					if first {
						baseEventId = binary.BigEndian.Uint64(word[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
						first = false
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
				}

				rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
					KeyCount:   blockCount,
					Enums:      blockCount > 0,
					BucketSize: 2000,
					LeafSize:   8,
					TmpDir:     tmpDir,
					IndexFile:  filepath.Join(sn.Dir(), snaptype.IdxFileName(sn.Version, sn.From, sn.To, snaptype.Enums.BorEvents.String())),
					BaseDataID: baseEventId,
				}, logger)
				if err != nil {
					return err
				}
				rs.LogLvl(log.LvlDebug)

				defer d.EnableMadvNormal().DisableReadAhead()
			RETRY:
				g.Reset(0)
				first = true
				var i, offset, nextPos uint64
				for g.HasNext() {
					word, nextPos = g.Next(word[:0])
					i++
					if first || !bytes.Equal(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum]) {
						if err = rs.AddKey(word[:length.Hash], offset); err != nil {
							return err
						}
						copy(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum])
					}
					if first {
						first = false
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					offset = nextPos
				}
				if err = rs.Build(ctx); err != nil {
					if errors.Is(err, recsplit.ErrCollision) {
						logger.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
						rs.ResetNextSalt()
						goto RETRY
					}
					return err
				}

				return nil
			}))

	BorSpans = snaptype.RegisterType(
		snaptype.Enums.BorSpans,
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		[]snaptype.Index{snaptype.Indexes.BorSpanId},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, sn snaptype.FileInfo, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						err = fmt.Errorf("BorSpansIdx: at=%d-%d, %v, %s", sn.From, sn.To, rec, dbg.Stack())
					}
				}()
				// Calculate how many records there will be in the index
				d, err := seg.NewDecompressor(sn.Path)
				if err != nil {
					return err
				}
				defer d.Close()

				baseSpanId := heimdall.SpanIdAt(sn.From)

				rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
					KeyCount:   d.Count(),
					Enums:      d.Count() > 0,
					BucketSize: 2000,
					LeafSize:   8,
					TmpDir:     tmpDir,
					IndexFile:  filepath.Join(sn.Dir(), sn.Type.IdxFileName(sn.Version, sn.From, sn.To)),
					BaseDataID: uint64(baseSpanId),
				}, logger)
				if err != nil {
					return err
				}
				rs.LogLvl(log.LvlDebug)

				defer d.EnableMadvNormal().DisableReadAhead()
			RETRY:
				g := d.MakeGetter()
				var i, offset, nextPos uint64
				var key [8]byte
				for g.HasNext() {
					nextPos, _ = g.Skip()
					binary.BigEndian.PutUint64(key[:], i)
					i++
					if err = rs.AddKey(key[:], offset); err != nil {
						return err
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					offset = nextPos
				}
				if err = rs.Build(ctx); err != nil {
					if errors.Is(err, recsplit.ErrCollision) {
						logger.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
						rs.ResetNextSalt()
						goto RETRY
					}
					return err
				}

				return nil
			}))

	BorSnapshotTypes = []snaptype.Type{BorEvents, BorSpans}
)
