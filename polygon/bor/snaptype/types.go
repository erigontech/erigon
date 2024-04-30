package snaptype

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/erigon/core/rawdb"
	coresnaptype "github.com/ledgerwatch/erigon/core/snaptype"
	bortypes "github.com/ledgerwatch/erigon/polygon/bor/types"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	borTypes := append(coresnaptype.BlockSnapshotTypes, BorSnapshotTypes...)

	snapcfg.RegisterKnownTypes(networkname.MumbaiChainName, borTypes)
	snapcfg.RegisterKnownTypes(networkname.AmoyChainName, borTypes)
	snapcfg.RegisterKnownTypes(networkname.BorMainnetChainName, borTypes)
}

var Enums = struct {
	snaptype.Enums
	BorEvents,
	BorSpans,
	BorCheckpoints,
	BorMilestones snaptype.Enum
}{
	Enums:          snaptype.Enums{},
	BorEvents:      snaptype.MinBorEnum,
	BorSpans:       snaptype.MinBorEnum + 1,
	BorCheckpoints: snaptype.MinBorEnum + 2,
	BorMilestones:  snaptype.MinBorEnum + 3,
}

var Indexes = struct {
	BorTxnHash,
	BorSpanId,
	BorCheckpointId,
	BorMilestoneId snaptype.Index
}{
	BorTxnHash:      snaptype.Index{Name: "borevents"},
	BorSpanId:       snaptype.Index{Name: "borspans"},
	BorCheckpointId: snaptype.Index{Name: "borcheckpoints"},
	BorMilestoneId:  snaptype.Index{Name: "bormilestones"},
}

var (
	BorEvents = snaptype.RegisterType(
		Enums.BorEvents,
		"borevents",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		snaptype.RangeExtractorFunc(
			func(ctx context.Context, blockFrom, blockTo uint64, _ snaptype.FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
				logEvery := time.NewTicker(20 * time.Second)
				defer logEvery.Stop()

				from := hexutility.EncodeTs(blockFrom)
				var first bool = true
				var prevBlockNum uint64
				var startEventId uint64
				var lastEventId uint64
				if err := kv.BigChunks(db, kv.BorEventNums, from, func(tx kv.Tx, blockNumBytes, eventIdBytes []byte) (bool, error) {
					blockNum := binary.BigEndian.Uint64(blockNumBytes)
					if first {
						startEventId = binary.BigEndian.Uint64(eventIdBytes)
						first = false
						prevBlockNum = blockNum
					} else if blockNum != prevBlockNum {
						endEventId := binary.BigEndian.Uint64(eventIdBytes)
						blockHash, e := rawdb.ReadCanonicalHash(tx, prevBlockNum)
						if e != nil {
							return false, e
						}
						if e := extractEventRange(startEventId, endEventId, tx, prevBlockNum, blockHash, collect); e != nil {
							return false, e
						}
						startEventId = endEventId
						prevBlockNum = blockNum
					}
					if blockNum >= blockTo {
						return false, nil
					}
					lastEventId = binary.BigEndian.Uint64(eventIdBytes)
					select {
					case <-ctx.Done():
						return false, ctx.Err()
					case <-logEvery.C:
						var m runtime.MemStats
						if lvl >= log.LvlInfo {
							dbg.ReadMemStats(&m)
						}
						logger.Log(lvl, "[bor snapshots] Dumping bor events", "block num", blockNum,
							"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
						)
					default:
					}
					return true, nil
				}); err != nil {
					return 0, err
				}
				if lastEventId > startEventId {
					if err := db.View(ctx, func(tx kv.Tx) error {
						blockHash, e := rawdb.ReadCanonicalHash(tx, prevBlockNum)
						if e != nil {
							return e
						}
						return extractEventRange(startEventId, lastEventId+1, tx, prevBlockNum, blockHash, collect)
					}); err != nil {
						return 0, err
					}
				}

				return lastEventId, nil
			}),
		[]snaptype.Index{Indexes.BorTxnHash},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, sn snaptype.FileInfo, salt uint32, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
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
					IndexFile:  filepath.Join(sn.Dir(), snaptype.IdxFileName(sn.Version, sn.From, sn.To, Enums.BorEvents.String())),
					BaseDataID: baseEventId,
				}, logger)
				if err != nil {
					return err
				}
				rs.LogLvl(log.LvlDebug)

				defer d.EnableMadvNormal().DisableReadAhead()

				for {
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
							continue
						}
						return err
					}

					return nil
				}
			}))

	BorSpans = snaptype.RegisterType(
		Enums.BorSpans,
		"borspans",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		snaptype.RangeExtractorFunc(
			func(ctx context.Context, blockFrom, blockTo uint64, firstKeyGetter snaptype.FirstKeyGetter, db kv.RoDB, _ *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
				spanFrom := uint64(heimdall.SpanIdAt(blockFrom))
				spanTo := uint64(heimdall.SpanIdAt(blockTo))
				return extractValueRange(ctx, kv.BorSpans, spanFrom, spanTo, db, collect, workers, lvl, logger)
			}),
		[]snaptype.Index{Indexes.BorSpanId},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, sn snaptype.FileInfo, salt uint32, _ *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				d, err := seg.NewDecompressor(sn.Path)

				if err != nil {
					return err
				}
				defer d.Close()

				baseSpanId := uint64(heimdall.SpanIdAt(sn.From))

				return buildValueIndex(ctx, sn, salt, d, baseSpanId, tmpDir, p, lvl, logger)
			}),
	)

	BorCheckpoints = snaptype.RegisterType(
		Enums.BorCheckpoints,
		"borcheckpoints",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		snaptype.RangeExtractorFunc(
			func(ctx context.Context, blockFrom, blockTo uint64, firstKeyGetter snaptype.FirstKeyGetter, db kv.RoDB, _ *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
				var checkpointTo, checkpointFrom heimdall.CheckpointId

				err := db.View(ctx, func(tx kv.Tx) (err error) {
					checkpointFrom, err = heimdall.CheckpointIdAt(tx, blockFrom)

					if err != nil {
						return err
					}

					checkpointTo, err = heimdall.CheckpointIdAt(tx, blockTo)

					if err != nil {
						return err
					}

					if blockFrom > 0 {
						if prevTo, err := heimdall.CheckpointIdAt(tx, blockFrom-1); err == nil {
							if prevTo == checkpointFrom {
								if prevTo == checkpointTo {
									checkpointFrom = 0
									checkpointTo = 0
								} else {
									checkpointFrom++
								}
							}
						}
					}

					return err
				})

				if err != nil {
					return 0, err
				}

				return extractValueRange(ctx, kv.BorCheckpoints, uint64(checkpointFrom), uint64(checkpointTo), db, collect, workers, lvl, logger)
			}),
		[]snaptype.Index{Indexes.BorCheckpointId},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, sn snaptype.FileInfo, salt uint32, _ *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				d, err := seg.NewDecompressor(sn.Path)

				if err != nil {
					return err
				}
				defer d.Close()

				gg := d.MakeGetter()

				var firstCheckpointId uint64

				if gg.HasNext() {
					buf, _ := d.MakeGetter().Next(nil)
					var firstCheckpoint heimdall.Checkpoint

					if err = json.Unmarshal(buf, &firstCheckpoint); err != nil {
						return err
					}

					firstCheckpointId = uint64(firstCheckpoint.Id)
				}

				return buildValueIndex(ctx, sn, salt, d, firstCheckpointId, tmpDir, p, lvl, logger)
			}),
	)

	BorMilestones = snaptype.RegisterType(
		Enums.BorMilestones,
		"bormilestones",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		snaptype.RangeExtractorFunc(
			func(ctx context.Context, blockFrom, blockTo uint64, firstKeyGetter snaptype.FirstKeyGetter, db kv.RoDB, _ *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
				var milestoneFrom, milestoneTo heimdall.MilestoneId

				err := db.View(ctx, func(tx kv.Tx) (err error) {
					milestoneFrom, err = heimdall.MilestoneIdAt(tx, blockFrom)

					if err != nil && !errors.Is(err, heimdall.ErrMilestoneNotFound) {
						return err
					}

					milestoneTo, err = heimdall.MilestoneIdAt(tx, blockTo)

					if err != nil && !errors.Is(err, heimdall.ErrMilestoneNotFound) {
						return err
					}

					if milestoneFrom > 0 && blockFrom > 0 {
						if prevTo, err := heimdall.MilestoneIdAt(tx, blockFrom-1); err == nil && prevTo == milestoneFrom {
							if prevTo == milestoneFrom {
								if prevTo == milestoneTo {
									milestoneFrom = 0
									milestoneTo = 0
								} else {
									milestoneFrom++
								}
							}
						}
					}

					return nil
				})

				if err != nil {
					return 0, err
				}

				return extractValueRange(ctx, kv.BorMilestones, uint64(milestoneFrom), uint64(milestoneTo), db, collect, workers, lvl, logger)
			}),
		[]snaptype.Index{Indexes.BorMilestoneId},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, sn snaptype.FileInfo, salt uint32, _ *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				d, err := seg.NewDecompressor(sn.Path)

				if err != nil {
					return err
				}
				defer d.Close()

				gg := d.MakeGetter()

				var firstMilestoneId uint64

				if gg.HasNext() {
					buf, _ := gg.Next(nil)
					if len(buf) > 0 {
						var firstMilestone heimdall.Milestone
						if err = json.Unmarshal(buf, &firstMilestone); err != nil {
							return err
						}
						firstMilestoneId = uint64(firstMilestone.Id)
					}
				}

				return buildValueIndex(ctx, sn, salt, d, firstMilestoneId, tmpDir, p, lvl, logger)
			}),
	)

	BorSnapshotTypes = []snaptype.Type{BorEvents, BorSpans, BorCheckpoints, BorMilestones}
)

func extractValueRange(ctx context.Context, table string, valueFrom, valueTo uint64, db kv.RoDB, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	if err := kv.BigChunks(db, table, hexutility.EncodeTs(valueFrom), func(tx kv.Tx, idBytes, valueBytes []byte) (bool, error) {
		id := binary.BigEndian.Uint64(idBytes)
		if id >= valueTo {
			return false, nil
		}
		if e := collect(valueBytes); e != nil {
			return false, e
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[bor snapshots] Dumping bor values", "id", id,
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return valueTo, err
	}
	return valueTo, nil
}

func buildValueIndex(ctx context.Context, sn snaptype.FileInfo, salt uint32, d *seg.Decompressor, baseId uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("BorSpansIdx: at=%d-%d, %v, %s", sn.From, sn.To, rec, dbg.Stack())
		}
	}()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      d.Count() > 0,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(sn.Dir(), sn.Type.IdxFileName(sn.Version, sn.From, sn.To)),
		BaseDataID: baseId,
		Salt:       salt,
	}, logger)
	if err != nil {
		return err
	}
	rs.LogLvl(log.LvlDebug)

	defer d.EnableMadvNormal().DisableReadAhead()

	for {
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
				continue
			}
			return err
		}

		return nil
	}
}

func extractEventRange(startEventId, endEventId uint64, tx kv.Tx, blockNum uint64, blockHash common.Hash, collect func([]byte) error) error {
	var blockNumBuf [8]byte
	var eventIdBuf [8]byte
	txnHash := bortypes.ComputeBorTxHash(blockNum, blockHash)
	binary.BigEndian.PutUint64(blockNumBuf[:], blockNum)
	for eventId := startEventId; eventId < endEventId; eventId++ {
		binary.BigEndian.PutUint64(eventIdBuf[:], eventId)
		event, err := tx.GetOne(kv.BorEvents, eventIdBuf[:])
		if err != nil {
			return err
		}
		snapshotRecord := make([]byte, len(event)+length.Hash+length.BlockNum+8)
		copy(snapshotRecord, txnHash[:])
		copy(snapshotRecord[length.Hash:], blockNumBuf[:])
		binary.BigEndian.PutUint64(snapshotRecord[length.Hash+length.BlockNum:], eventId)
		copy(snapshotRecord[length.Hash+length.BlockNum+8:], event)
		if err := collect(snapshotRecord); err != nil {
			return err
		}
	}
	return nil
}
