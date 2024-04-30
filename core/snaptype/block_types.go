package snaptype

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/crypto/cryptopool"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	ethereumTypes := append(BlockSnapshotTypes, snaptype.CaplinSnapshotTypes...)

	snapcfg.RegisterKnownTypes(networkname.MainnetChainName, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.SepoliaChainName, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.GoerliChainName, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.GnosisChainName, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.ChiadoChainName, ethereumTypes)
}

var Enums = struct {
	snaptype.Enums
	Headers,
	Bodies,
	Transactions snaptype.Enum
}{
	Enums:        snaptype.Enums{},
	Headers:      snaptype.MinCoreEnum,
	Bodies:       snaptype.MinCoreEnum + 1,
	Transactions: snaptype.MinCoreEnum + 2,
}

var Indexes = struct {
	HeaderHash,
	BodyHash,
	TxnHash,
	TxnHash2BlockNum snaptype.Index
}{
	HeaderHash:       snaptype.Index{Name: "headers"},
	BodyHash:         snaptype.Index{Name: "bodies"},
	TxnHash:          snaptype.Index{Name: "transactions"},
	TxnHash2BlockNum: snaptype.Index{Name: "transactions-to-block", Offset: 1},
}

var (
	Headers = snaptype.RegisterType(
		Enums.Headers,
		"headers",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		[]snaptype.Index{Indexes.HeaderHash},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, info snaptype.FileInfo, salt uint32, _ *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				hasher := crypto.NewKeccakState()
				defer cryptopool.ReturnToPoolKeccak256(hasher)
				var h common.Hash
				if err := snaptype.BuildIndex(ctx, info, salt, info.From, tmpDir, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
					if p != nil {
						p.Processed.Add(1)
					}

					headerRlp := word[1:]
					hasher.Reset()
					hasher.Write(headerRlp)
					hasher.Read(h[:])
					if err := idx.AddKey(h[:], offset); err != nil {
						return err
					}
					return nil
				}, logger); err != nil {
					return fmt.Errorf("HeadersIdx: %w", err)
				}
				return nil
			}),
	)

	Bodies = snaptype.RegisterType(
		Enums.Bodies,
		"bodies",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		[]snaptype.Index{Indexes.BodyHash},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, info snaptype.FileInfo, salt uint32, _ *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				num := make([]byte, 8)

				if err := snaptype.BuildIndex(ctx, info, salt, info.From, tmpDir, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, _ []byte) error {
					if p != nil {
						p.Processed.Add(1)
					}
					n := binary.PutUvarint(num, i)
					if err := idx.AddKey(num[:n], offset); err != nil {
						return err
					}
					return nil
				}, logger); err != nil {
					return fmt.Errorf("can't index %s: %w", info.Name(), err)
				}
				return nil
			}),
	)

	Transactions = snaptype.RegisterType(
		Enums.Transactions,
		"transactions",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		[]snaptype.Index{Indexes.TxnHash, Indexes.TxnHash2BlockNum},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, sn snaptype.FileInfo, salt uint32, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						err = fmt.Errorf("index panic: at=%s, %v, %s", sn.Name(), rec, dbg.Stack())
					}
				}()
				firstBlockNum := sn.From

				bodiesSegment, err := seg.NewDecompressor(sn.As(Bodies).Path)
				if err != nil {
					return fmt.Errorf("can't open %s for indexing: %w", sn.As(Bodies).Name(), err)
				}
				defer bodiesSegment.Close()

				firstTxID, expectedCount, err := txsAmountBasedOnBodiesSnapshots(bodiesSegment, sn.Len()-1)
				if err != nil {
					return err
				}

				d, err := seg.NewDecompressor(sn.Path)
				if err != nil {
					return fmt.Errorf("can't open %s for indexing: %w", sn.Path, err)
				}
				defer d.Close()
				if d.Count() != expectedCount {
					return fmt.Errorf("TransactionsIdx: at=%d-%d, pre index building, expect: %d, got %d", sn.From, sn.To, expectedCount, d.Count())
				}

				if p != nil {
					name := sn.Name()
					p.Name.Store(&name)
					p.Total.Store(uint64(d.Count() * 2))
				}

				txnHashIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
					KeyCount: d.Count(),

					Enums:              true,
					LessFalsePositives: true,

					BucketSize: 2000,
					LeafSize:   8,
					TmpDir:     tmpDir,
					IndexFile:  filepath.Join(sn.Dir(), sn.Type.IdxFileName(sn.Version, sn.From, sn.To)),
					BaseDataID: firstTxID,
				}, logger)
				if err != nil {
					return err
				}

				txnHash2BlockNumIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
					KeyCount:   d.Count(),
					Enums:      false,
					BucketSize: 2000,
					LeafSize:   8,
					TmpDir:     tmpDir,
					IndexFile:  filepath.Join(sn.Dir(), sn.Type.IdxFileName(sn.Version, sn.From, sn.To, Indexes.TxnHash2BlockNum)),
					BaseDataID: firstBlockNum,
				}, logger)
				if err != nil {
					return err
				}
				txnHashIdx.LogLvl(log.LvlDebug)
				txnHash2BlockNumIdx.LogLvl(log.LvlDebug)

				chainId, _ := uint256.FromBig(chainConfig.ChainID)

				parseCtx := types2.NewTxParseContext(*chainId)
				parseCtx.WithSender(false)
				slot := types2.TxSlot{}
				bodyBuf, word := make([]byte, 0, 4096), make([]byte, 0, 4096)

				defer d.EnableMadvNormal().DisableReadAhead()
				defer bodiesSegment.EnableMadvNormal().DisableReadAhead()

				for {
					g, bodyGetter := d.MakeGetter(), bodiesSegment.MakeGetter()
					var i, offset, nextPos uint64
					blockNum := firstBlockNum
					body := &types.BodyForStorage{}

					bodyBuf, _ = bodyGetter.Next(bodyBuf[:0])
					if err := rlp.DecodeBytes(bodyBuf, body); err != nil {
						return err
					}

					for g.HasNext() {
						if p != nil {
							p.Processed.Add(1)
						}

						word, nextPos = g.Next(word[:0])
						select {
						case <-ctx.Done():
							return ctx.Err()
						default:
						}

						for body.BaseTxId+uint64(body.TxAmount) <= firstTxID+i { // skip empty blocks
							if !bodyGetter.HasNext() {
								return fmt.Errorf("not enough bodies")
							}

							bodyBuf, _ = bodyGetter.Next(bodyBuf[:0])
							if err := rlp.DecodeBytes(bodyBuf, body); err != nil {
								return err
							}

							blockNum++
						}

						firstTxByteAndlengthOfAddress := 21
						isSystemTx := len(word) == 0
						if isSystemTx { // system-txs hash:pad32(txnID)
							slot.IDHash = common.Hash{}
							binary.BigEndian.PutUint64(slot.IDHash[:], firstTxID+i)
						} else {
							if _, err = parseCtx.ParseTransaction(word[firstTxByteAndlengthOfAddress:], 0, &slot, nil, true /* hasEnvelope */, false /* wrappedWithBlobs */, nil /* validateHash */); err != nil {
								return fmt.Errorf("ParseTransaction: %w, blockNum: %d, i: %d", err, blockNum, i)
							}
						}

						if err := txnHashIdx.AddKey(slot.IDHash[:], offset); err != nil {
							return err
						}
						if err := txnHash2BlockNumIdx.AddKey(slot.IDHash[:], blockNum); err != nil {
							return err
						}

						i++
						offset = nextPos
					}

					if int(i) != expectedCount {
						return fmt.Errorf("TransactionsIdx: at=%d-%d, post index building, expect: %d, got %d", sn.From, sn.To, expectedCount, i)
					}

					if err := txnHashIdx.Build(ctx); err != nil {
						if errors.Is(err, recsplit.ErrCollision) {
							logger.Warn("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
							txnHashIdx.ResetNextSalt()
							txnHash2BlockNumIdx.ResetNextSalt()
							continue
						}
						return fmt.Errorf("txnHashIdx: %w", err)
					}
					if err := txnHash2BlockNumIdx.Build(ctx); err != nil {
						if errors.Is(err, recsplit.ErrCollision) {
							logger.Warn("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
							txnHashIdx.ResetNextSalt()
							txnHash2BlockNumIdx.ResetNextSalt()
							continue
						}
						return fmt.Errorf("txnHash2BlockNumIdx: %w", err)
					}

					return nil
				}
			}),
	)

	BlockSnapshotTypes = []snaptype.Type{Headers, Bodies, Transactions}
)

func txsAmountBasedOnBodiesSnapshots(bodiesSegment *seg.Decompressor, len uint64) (firstTxID uint64, expectedCount int, err error) {
	gg := bodiesSegment.MakeGetter()
	buf, _ := gg.Next(nil)
	firstBody := &types.BodyForStorage{}
	if err = rlp.DecodeBytes(buf, firstBody); err != nil {
		return
	}
	firstTxID = firstBody.BaseTxId

	lastBody := new(types.BodyForStorage)
	i := uint64(0)
	for gg.HasNext() {
		i++
		if i == len {
			buf, _ = gg.Next(buf[:0])
			if err = rlp.DecodeBytes(buf, lastBody); err != nil {
				return
			}
			if gg.HasNext() {
				panic(1)
			}
		} else {
			gg.Skip()
		}
	}

	if lastBody.BaseTxId < firstBody.BaseTxId {
		return 0, 0, fmt.Errorf("negative txs count %s: lastBody.BaseTxId=%d < firstBody.BaseTxId=%d", bodiesSegment.FileName(), lastBody.BaseTxId, firstBody.BaseTxId)
	}

	expectedCount = int(lastBody.BaseTxId+uint64(lastBody.TxAmount)) - int(firstBody.BaseTxId)
	return
}
