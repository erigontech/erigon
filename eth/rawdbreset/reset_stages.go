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

package rawdbreset

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
)

func ResetState(db kv.TemporalRwDB, ctx context.Context) error {
	// don't reset senders here
	if err := db.Update(ctx, ResetWitnesses); err != nil {
		return err
	}
	if err := db.Update(ctx, ResetTxLookup); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.CustomTrace); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.Finish); err != nil {
		return err
	}

	if err := ResetExec(ctx, db); err != nil {
		return err
	}
	return nil
}

func ResetBlocks(tx kv.RwTx, db kv.RoDB, br services.FullBlockReader, bw *blockio.BlockWriter, dirs datadir.Dirs, logger log.Logger) error {
	// keep Genesis
	if err := rawdb.TruncateBlocks(context.Background(), tx, 1); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, 1); err != nil {
		return fmt.Errorf("saving Bodies progress failed: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, 1); err != nil {
		return fmt.Errorf("saving Bodies progress failed: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Snapshots, 0); err != nil {
		return fmt.Errorf("saving Snapshots progress failed: %w", err)
	}

	// remove all canonical markers from this point
	if err := rawdb.TruncateCanonicalHash(tx, 1, false /* markChainAsBad */); err != nil {
		return err
	}
	if err := rawdb.TruncateTd(tx, 1); err != nil {
		return err
	}
	hash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return err
	}
	if err = rawdb.WriteHeadHeaderHash(tx, hash); err != nil {
		return err
	}

	// ensure no garbage records left (it may happen if db is inconsistent)
	if err := bw.TruncateBodies(db, tx, 2); err != nil {
		return err
	}

	if br.FrozenBlocks() > 0 {
		logger.Info("filling db from snapshots", "blocks", br.FrozenBlocks())
		if err := FillDBFromSnapshots("filling_db_from_snapshots", context.Background(), tx, dirs, br, logger); err != nil {
			return err
		}
		_ = stages.SaveStageProgress(tx, stages.Snapshots, br.FrozenBlocks())
		_ = stages.SaveStageProgress(tx, stages.Headers, br.FrozenBlocks())
		_ = stages.SaveStageProgress(tx, stages.Bodies, br.FrozenBlocks())
		_ = stages.SaveStageProgress(tx, stages.Senders, br.FrozenBlocks())
	}

	return nil
}

func ResetSenders(ctx context.Context, tx kv.RwTx) error {
	if err := backup.ClearTables(ctx, tx, kv.Senders); err != nil {
		return nil
	}
	return clearStageProgress(tx, stages.Senders)
}

func ResetExec(ctx context.Context, db kv.TemporalRwDB) (err error) {
	cleanupList := make([]string, 0)
	cleanupList = append(cleanupList, stateBuckets...)
	cleanupList = append(cleanupList, stateHistoryBuckets...)
	cleanupList = append(cleanupList, db.Debug().DomainTables(kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain, kv.ReceiptDomain, kv.RCacheDomain)...)
	cleanupList = append(cleanupList, db.Debug().InvertedIdxTables(kv.LogAddrIdx, kv.LogTopicIdx, kv.TracesFromIdx, kv.TracesToIdx)...)

	return db.Update(ctx, func(tx kv.RwTx) error {
		if err := clearStageProgress(tx, stages.Execution); err != nil {
			return err
		}

		if err := backup.ClearTables(ctx, tx, cleanupList...); err != nil {
			return nil
		}
		// corner case: state files may be ahead of block files - so, can't use SharedDomains here. juts leave progress as 0.
		return nil
	})
}

func ResetTxLookup(tx kv.RwTx) error {
	if err := tx.ClearTable(kv.TxLookup); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	return nil
}

func ResetWitnesses(tx kv.RwTx) error {
	if err := tx.ClearTable(kv.BorWitnesses); err != nil {
		return err
	}
	if err := tx.ClearTable(kv.BorWitnessSizes); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.WitnessProcessing, 0); err != nil {
		return err
	}
	return nil
}

var Tables = map[stages.SyncStage][]string{
	stages.CustomTrace: {},
	stages.Finish:      {},
}
var stateBuckets = []string{
	kv.Epoch, kv.PendingEpoch, kv.Code,
}
var stateHistoryBuckets = []string{
	kv.TblPruningProgress,
	kv.ChangeSets3,
}

func clearStageProgress(tx kv.RwTx, stagesList ...stages.SyncStage) error {
	for _, stage := range stagesList {
		if err := stages.SaveStageProgress(tx, stage, 0); err != nil {
			return err
		}
		if err := stages.SaveStagePruneProgress(tx, stage, 0); err != nil {
			return err
		}
	}
	return nil
}

func Reset(ctx context.Context, db kv.RwDB, stagesList ...stages.SyncStage) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		for _, st := range stagesList {
			if err := backup.ClearTables(ctx, tx, Tables[st]...); err != nil {
				return err
			}
			if err := clearStageProgress(tx, stagesList...); err != nil {
				return err
			}
		}
		return nil
	})
}

func FillDBFromSnapshots(logPrefix string, ctx context.Context, tx kv.RwTx, dirs datadir.Dirs, blockReader services.FullBlockReader, logger log.Logger) error {
	startTime := time.Now()
	blocksAvailable := blockReader.FrozenBlocks()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	pruneMarkerBlockThreshold := GetPruneMarkerSafeThreshold(blockReader)

	// updating the progress of further stages (but only forward) that are contained inside of snapshots
	for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies, stages.BlockHashes, stages.Senders} {
		progress, err := stages.GetStageProgress(tx, stage)

		if err != nil {
			return fmt.Errorf("get %s stage progress to advance: %w", stage, err)
		}
		if progress >= blocksAvailable {
			continue
		}

		if err = stages.SaveStageProgress(tx, stage, blocksAvailable); err != nil {
			return fmt.Errorf("advancing %s stage: %w", stage, err)
		}

		switch stage {
		case stages.Headers:
			h2n := etl.NewCollector(logPrefix, dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize/2), logger)
			defer h2n.Close()
			h2n.SortAndFlushInBackground(true)
			h2n.LogLvl(log.LvlDebug)

			// fill some small tables from snapshots, in future we may store this data in snapshots also, but
			// for now easier just store them in db
			td := big.NewInt(0)
			blockNumBytes := make([]byte, 8)
			if err := blockReader.HeadersRange(ctx, func(header *types.Header) error {
				blockNum, blockHash := header.Number.Uint64(), header.Hash()
				td.Add(td, header.Difficulty)
				// What can happen if chaindata is deleted is that maybe header.seg progress is lower or higher than
				// body.seg progress. In this case we need to skip the header, and "normalize" the progress to keep them in sync.
				if blockNum > blocksAvailable {
					return nil // This can actually happen as FrozenBlocks() is SegmentIdMax() and not the last .seg
				}
				if !dbg.PruneTotalDifficulty() {
					if err := rawdb.WriteTd(tx, blockHash, blockNum, td); err != nil {
						return err
					}
				}

				// Write marker for pruning only if we are above our safe threshold
				if blockNum >= pruneMarkerBlockThreshold || blockNum == 0 {
					if err := rawdb.WriteCanonicalHash(tx, blockHash, blockNum); err != nil {
						return err
					}
					binary.BigEndian.PutUint64(blockNumBytes, blockNum)
					if err := h2n.Collect(blockHash[:], blockNumBytes); err != nil {
						return err
					}
					if dbg.PruneTotalDifficulty() {
						if err := rawdb.WriteTd(tx, blockHash, blockNum, td); err != nil {
							return err
						}
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					diaglib.Send(diaglib.SnapshotFillDBStageUpdate{
						Stage: diaglib.SnapshotFillDBStage{
							StageName: string(stage),
							Current:   header.Number.Uint64(),
							Total:     blocksAvailable,
						},
						TimeElapsed: time.Since(startTime).Seconds(),
					})
					logger.Info(fmt.Sprintf("[%s] Total difficulty index: %s/%s", logPrefix,
						common.PrettyCounter(header.Number.Uint64()), common.PrettyCounter(blockReader.FrozenBlocks())))
				default:
				}
				return nil
			}); err != nil {
				return err
			}
			if err := h2n.Load(tx, kv.HeaderNumber, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
				return err
			}
			canonicalHash, ok, err := blockReader.CanonicalHash(ctx, tx, blocksAvailable)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("canonical marker not found: %d", blocksAvailable)
			}
			if err = rawdb.WriteHeadHeaderHash(tx, canonicalHash); err != nil {
				return err
			}

		case stages.Bodies:
			firstTxNum := blockReader.FirstTxnNumNotInSnapshots()
			if err := tx.ResetSequence(kv.EthTx, firstTxNum); err != nil {
				return err
			}

			_ = tx.ClearTable(kv.MaxTxNum)
			if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					diaglib.Send(diaglib.SnapshotFillDBStageUpdate{
						Stage: diaglib.SnapshotFillDBStage{
							StageName: string(stage),
							Current:   blockNum,
							Total:     blocksAvailable,
						},
						TimeElapsed: time.Since(startTime).Seconds(),
					})
					logger.Info(fmt.Sprintf("[%s] MaxTxNums index: %s/%s", logPrefix, common.PrettyCounter(blockNum), common.PrettyCounter(blockReader.FrozenBlocks())))
				default:
				}
				if baseTxNum+txAmount == 0 {
					panic(baseTxNum + txAmount) //uint-underflow
				}
				maxTxNum := baseTxNum + txAmount - 1
				// What can happen if chaindata is deleted is that maybe header.seg progress is lower or higher than
				// body.seg progress. In this case we need to skip the header, and "normalize" the progress to keep them in sync.
				if blockNum > blocksAvailable {
					return nil // This can actually happen as FrozenBlocks() is SegmentIdMax() and not the last .seg
				}
				if blockNum >= pruneMarkerBlockThreshold || blockNum == 0 {
					if err := rawdbv3.TxNums.Append(tx, blockNum, maxTxNum); err != nil {
						return fmt.Errorf("%w. blockNum=%d, maxTxNum=%d", err, blockNum, maxTxNum)
					}
				}
				return nil
			}); err != nil {
				return fmt.Errorf("build txNum => blockNum mapping: %w", err)
			}
			if blockReader.FrozenBlocks() > 0 {
				if err := rawdb.AppendCanonicalTxNums(tx, blockReader.FrozenBlocks()+1); err != nil {
					return err
				}
			} else {
				if err := rawdb.AppendCanonicalTxNums(tx, 0); err != nil {
					return err
				}
			}

		default:
			diaglib.Send(diaglib.SnapshotFillDBStageUpdate{
				Stage: diaglib.SnapshotFillDBStage{
					StageName: string(stage),
					Current:   blocksAvailable, // as we are done with other stages
					Total:     blocksAvailable,
				},
				TimeElapsed: time.Since(startTime).Seconds(),
			})
		}
	}
	return nil
}

const (
	/*
		we strive to read indexes from snapshots instead to db... this means that there can be sometimes (e.g when we merged past indexes),
		a situation when we need to read indexes and we choose to read them from either a corrupt index or an incomplete index.
		so we need to extend the threshold to > max_merge_segment_size.
	*/
	pruneMarkerSafeThreshold = snaptype.Erigon2MergeLimit * 1.5 // 1.5x the merge limit
)

func GetPruneMarkerSafeThreshold(blockReader services.FullBlockReader) uint64 {
	snapProgress := min(blockReader.FrozenBorBlocks(false), blockReader.FrozenBlocks())
	if blockReader.BorSnapshots() == nil {
		snapProgress = blockReader.FrozenBlocks()
	}
	if snapProgress < pruneMarkerSafeThreshold {
		return 0
	}
	return snapProgress - pruneMarkerSafeThreshold
}
