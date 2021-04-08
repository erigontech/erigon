package snapshotsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var (
	HeaderNumber    = stages.SyncStage("snapshot_header_number")
	HeaderCanonical = stages.SyncStage("snapshot_canonical")
)

func PostProcessing(db ethdb.Database, mode SnapshotMode, downloadedSnapshots map[SnapshotType]*SnapshotsInfo) error {
	if mode.Headers {
		err := GenerateHeaderIndexes(context.Background(), db)
		if err != nil {
			return err
		}
	}

	if mode.Bodies {
		err := PostProcessBodies(db)
		if err != nil {
			return err
		}
	}

	if mode.State {
		err := PostProcessState(db, downloadedSnapshots[SnapshotType_state])
		if err != nil {
			return err
		}
	}
	return nil
}

func PostProcessBodies(db ethdb.Database) error {
	v, err := stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return err
	}

	if v > 0 {
		return nil
	}

	k, body, err := db.Last(dbutils.BlockBodyPrefix)
	if err != nil {
		return err
	}

	if body == nil {
		return fmt.Errorf("empty body for key %s", common.Bytes2Hex(k))
	}

	number := binary.BigEndian.Uint64(k[:8])
	err = stages.SaveStageProgress(db, stages.Bodies, number)
	if err != nil {
		return err
	}
	return nil
}

func PostProcessState(db ethdb.GetterPutter, info *SnapshotsInfo) error {
	v, err := stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		return err
	}

	if v > 0 {
		return nil
	}

	err = stages.SaveStageProgress(db, stages.Execution, info.SnapshotBlock)
	if err != nil {
		return err
	}
	return nil
}

func generateHeaderIndexesStep1(ctx context.Context, db ethdb.Database) error {
	v, err1 := stages.GetStageProgress(db, HeaderNumber)
	if err1 != nil {
		return err1
	}

	if v == 0 {
		tx, err := db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		log.Info("Generate headers hash to number index")
		headHashBytes, innerErr := tx.Get(dbutils.HeadersSnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadHash))
		if innerErr != nil {
			return innerErr
		}

		headNumberBytes, innerErr := tx.Get(dbutils.HeadersSnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadNumber))
		if innerErr != nil {
			return innerErr
		}

		headNumber := big.NewInt(0).SetBytes(headNumberBytes).Uint64()
		headHash := common.BytesToHash(headHashBytes)

		innerErr = etl.Transform("Torrent post-processing 1", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeadersBucket, dbutils.HeaderNumberBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			return next(k, common.CopyBytes(k[8:]), common.CopyBytes(k[:8]))
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit:          ctx.Done(),
			ExtractEndKey: dbutils.HeaderKey(headNumber, headHash),
		})
		if innerErr != nil {
			return innerErr
		}
		if err = stages.SaveStageProgress(tx, HeaderNumber, 1); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func generateHeaderIndexesStep2(ctx context.Context, db ethdb.Database) error {
	var hash common.Hash
	var number uint64

	v, err1 := stages.GetStageProgress(db, HeaderCanonical)
	if err1 != nil {
		return err1
	}
	if v == 0 {
		tx, err := db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		h := rawdb.ReadHeaderByNumber(tx, 0)
		td := h.Difficulty

		log.Info("Generate TD index & canonical")
		err = etl.Transform("Torrent post-processing 2", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeadersBucket, dbutils.HeaderTDBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			header := &types.Header{}
			innerErr := rlp.DecodeBytes(v, header)
			if innerErr != nil {
				return innerErr
			}
			number = header.Number.Uint64()
			hash = header.Hash()
			td = td.Add(td, header.Difficulty)
			tdBytes, innerErr := rlp.EncodeToBytes(td)
			if innerErr != nil {
				return innerErr
			}

			return next(k, dbutils.HeaderKey(header.Number.Uint64(), header.Hash()), tdBytes)
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: ctx.Done(),
		})
		if err != nil {
			return err
		}
		log.Info("Generate TD index & canonical")
		err = etl.Transform("Torrent post-processing 2", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeadersBucket, dbutils.HeaderCanonicalBucket, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			return next(k, common.CopyBytes(k[:8]), common.CopyBytes(k[8:]))
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: ctx.Done(),
		})
		if err != nil {
			return err
		}
		rawdb.WriteHeadHeaderHash(tx, hash)
		rawdb.WriteHeaderNumber(tx, hash, number)
		err = stages.SaveStageProgress(tx, stages.Headers, number)
		if err != nil {
			return err
		}
		err = stages.SaveStageProgress(tx, stages.BlockHashes, number)
		if err != nil {
			return err
		}
		rawdb.WriteHeadBlockHash(tx, hash)
		if err = stages.SaveStageProgress(tx, HeaderCanonical, number); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		log.Info("Last processed block", "num", number, "hash", hash.String())
	}

	return nil
}

func GenerateHeaderIndexes(ctx context.Context, db ethdb.Database) error {
	if err := generateHeaderIndexesStep1(ctx, db); err != nil {
		return err
	}
	if err := generateHeaderIndexesStep2(ctx, db); err != nil {
		return err
	}
	return nil
}
