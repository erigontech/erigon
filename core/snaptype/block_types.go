// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package snaptype

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/crypto/cryptopool"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

func init() {
	ethereumTypes := append(BlockSnapshotTypes, snaptype.CaplinSnapshotTypes...)

	snapcfg.RegisterKnownTypes(networkname.Mainnet, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.Sepolia, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.Gnosis, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.Chiado, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.Holesky, ethereumTypes)
	snapcfg.RegisterKnownTypes(networkname.TaikoAlethia, ethereumTypes) // CHANGE(taiko) : register taiko
}

var Enums = struct {
	snaptype.Enums
	Salt,
	Headers,
	Bodies,
	Transactions,
	Domains,
	Histories,
	InvertedIndicies,
	Accessor,
	Txt snaptype.Enum
}{
	Enums:            snaptype.Enums{},
	Salt:             snaptype.MinCoreEnum,
	Headers:          snaptype.MinCoreEnum + 1,
	Bodies:           snaptype.MinCoreEnum + 2,
	Transactions:     snaptype.MinCoreEnum + 3,
	Domains:          snaptype.MinCoreEnum + 4,
	Histories:        snaptype.MinCoreEnum + 5,
	InvertedIndicies: snaptype.MinCoreEnum + 6,
	Accessor:         snaptype.MinCoreEnum + 7,
	Txt:              snaptype.MinCoreEnum + 8,
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
	Salt = snaptype.RegisterType(
		Enums.Domains,
		"salt",
		snaptype.Versions{
			Current:      0, //2,
			MinSupported: 0,
		},
		nil,
		nil,
		nil,
	)
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

				cfg := recsplit.RecSplitArgs{
					Enums:              true,
					BucketSize:         recsplit.DefaultBucketSize,
					LeafSize:           recsplit.DefaultLeafSize,
					TmpDir:             tmpDir,
					Salt:               &salt,
					BaseDataID:         info.From,
					LessFalsePositives: true,
				}
				if err := snaptype.BuildIndex(ctx, info, cfg, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
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
				num := make([]byte, binary.MaxVarintLen64)

				cfg := recsplit.RecSplitArgs{
					Enums:      true,
					BucketSize: recsplit.DefaultBucketSize,
					LeafSize:   recsplit.DefaultLeafSize,
					TmpDir:     tmpDir,
					Salt:       &salt,
					BaseDataID: info.From,
				}
				if err := snaptype.BuildIndex(ctx, info, cfg, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, _ []byte) error {
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

				baseTxnID, expectedCount, err := txsAmountBasedOnBodiesSnapshots(bodiesSegment, sn.Len()-1)
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

					BucketSize: recsplit.DefaultBucketSize,
					LeafSize:   recsplit.DefaultLeafSize,
					TmpDir:     tmpDir,
					IndexFile:  filepath.Join(sn.Dir(), sn.Type.IdxFileName(sn.Version, sn.From, sn.To)),
					BaseDataID: baseTxnID.U64(),
				}, logger)
				if err != nil {
					return err
				}

				txnHash2BlockNumIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
					KeyCount:   d.Count(),
					Enums:      false,
					BucketSize: recsplit.DefaultBucketSize,
					LeafSize:   recsplit.DefaultLeafSize,
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

				parseCtx := txpool.NewTxnParseContext(*chainId)
				parseCtx.WithSender(false)
				slot := txpool.TxnSlot{}
				bodyBuf, word := make([]byte, 0, 4096), make([]byte, 0, 4096)

				defer d.EnableReadAhead().DisableReadAhead()
				defer bodiesSegment.EnableReadAhead().DisableReadAhead()

				for {
					g, bodyGetter := d.MakeGetter(), bodiesSegment.MakeGetter()
					var ti, offset, nextPos uint64
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

						// TODO review this code, test pass with lhs+1 <= baseTxnID.U64()+ti
						for body.BaseTxnID.LastSystemTx(body.TxCount) < baseTxnID.U64()+ti { // skip empty blocks; ti here is not transaction index in one block, but total transaction index counter
							if !bodyGetter.HasNext() {
								return errors.New("not enough bodies")
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
							binary.BigEndian.PutUint64(slot.IDHash[:], baseTxnID.U64()+ti)
						} else {
							if _, err = parseCtx.ParseTransaction(word[firstTxByteAndlengthOfAddress:], 0, &slot, nil, true /* hasEnvelope */, false /* wrappedWithBlobs */, nil /* validateHash */); err != nil {
								return fmt.Errorf("ParseTransaction: %w, blockNum: %d, i: %d", err, blockNum, ti)
							}
						}

						if err := txnHashIdx.AddKey(slot.IDHash[:], offset); err != nil {
							return err
						}
						if err := txnHash2BlockNumIdx.AddKey(slot.IDHash[:], blockNum); err != nil {
							return err
						}

						ti++
						offset = nextPos
					}

					if int(ti) != expectedCount {
						return fmt.Errorf("TransactionsIdx: at=%d-%d, post index building, expect: %d, got %d", sn.From, sn.To, expectedCount, ti)
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
	Domains = snaptype.RegisterType(
		Enums.Domains,
		"domain",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		nil,
		nil,
	)
	Histories = snaptype.RegisterType(
		Enums.Histories,
		"history",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		nil,
		nil,
	)
	InvertedIndicies = snaptype.RegisterType(
		Enums.InvertedIndicies,
		"idx",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		nil,
		nil,
	)

	Accessors = snaptype.RegisterType(
		Enums.Accessor,
		"accessor",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		nil,
		nil,
	)

	Txt = snaptype.RegisterType(
		Enums.Txt,
		"txt",
		snaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		nil,
		nil,
	)
	BlockSnapshotTypes = []snaptype.Type{Headers, Bodies, Transactions}
	E3StateTypes       = []snaptype.Type{Domains, Histories, InvertedIndicies, Accessors, Txt}
)

func txsAmountBasedOnBodiesSnapshots(bodiesSegment *seg.Decompressor, len uint64) (baseTxID types.BaseTxnID, expectedCount int, err error) {
	gg := bodiesSegment.MakeGetter()
	buf, _ := gg.Next(nil)
	firstBody := &types.BodyForStorage{}
	if err = rlp.DecodeBytes(buf, firstBody); err != nil {
		return
	}
	baseTxID = firstBody.BaseTxnID

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

	if lastBody.BaseTxnID < firstBody.BaseTxnID {
		return 0, 0, fmt.Errorf("negative txs count %s: lastBody.BaseTxId=%d < firstBody.BaseTxId=%d", bodiesSegment.FileName(), lastBody.BaseTxnID, firstBody.BaseTxnID)
	}

	// TODO: check if it is correct
	magic := uint64(1)
	expectedCount = int(lastBody.BaseTxnID.LastSystemTx(lastBody.TxCount) + magic - firstBody.BaseTxnID.U64())
	return
}
