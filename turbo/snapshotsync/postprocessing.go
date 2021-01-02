package snapshotsync

import (
	"context"
	"encoding/binary"
	"errors"
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

func GenerateHeaderIndexes(ctx context.Context, db ethdb.Database) error {
	var hash common.Hash
	var number uint64

	v, err := stages.GetStageProgress(db, HeaderNumber)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}

	if v == 0 {
		log.Info("Generate headers hash to number index")
		headHashBytes, innerErr := db.Get(dbutils.HeadersSnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadHash))
		if innerErr != nil {
			return innerErr
		}

		headNumberBytes, innerErr := db.Get(dbutils.HeadersSnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadNumber))
		if innerErr != nil {
			return innerErr
		}

		headNumber := big.NewInt(0).SetBytes(headNumberBytes).Uint64()
		headHash := common.BytesToHash(headHashBytes)

		innerErr = etl.Transform("Torrent post-processing 1", db, dbutils.HeaderPrefix, dbutils.HeaderNumberPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 8+common.HashLength {
				return nil
			}
			return next(k, common.CopyBytes(k[8:]), common.CopyBytes(k[:8]))
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: ctx.Done(),
			OnLoadCommit: func(db ethdb.Putter, key []byte, isDone bool) error {
				if !isDone {
					return nil
				}
				return stages.SaveStageProgress(db, HeaderNumber, 1)
			},
			ExtractEndKey: dbutils.HeaderKey(headNumber, headHash),
		})
		if innerErr != nil {
			return innerErr
		}
	}

	v, err = stages.GetStageProgress(db, HeaderCanonical)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}
	if v == 0 {
		h := rawdb.ReadHeaderByNumber(db, 0)
		td := h.Difficulty

		log.Info("Generate TD index & canonical")
		err = etl.Transform("Torrent post-processing 2", db, dbutils.HeaderPrefix, dbutils.HeaderPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 8+common.HashLength {
				return nil
			}
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

			innerErr = next(k, dbutils.HeaderTDKey(header.Number.Uint64(), header.Hash()), tdBytes)
			if innerErr != nil {
				return innerErr
			}

			//canonical
			return next(k, dbutils.HeaderHashKey(header.Number.Uint64()), header.Hash().Bytes())
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: ctx.Done(),
			OnLoadCommit: func(db ethdb.Putter, key []byte, isDone bool) error {
				if !isDone {
					return nil
				}

				rawdb.WriteHeadHeaderHash(db, hash)
				rawdb.WriteHeaderNumber(db, hash, number)
				err = stages.SaveStageProgress(db, stages.Headers, number)
				if err != nil {
					return err
				}
				err = stages.SaveStageProgress(db, stages.BlockHashes, number)
				if err != nil {
					return err
				}
				rawdb.WriteHeadBlockHash(db, hash)
				return stages.SaveStageProgress(db, HeaderCanonical, number)
			},
		})
		if err != nil {
			return err
		}
		log.Info("Last processed block", "num", number, "hash", hash.String())
	}

	return nil
}
