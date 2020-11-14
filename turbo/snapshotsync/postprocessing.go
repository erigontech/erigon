package snapshotsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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

func PostProcessing(db ethdb.Database, mode SnapshotMode) error {
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
	return nil
}

func PostProcessBodies(db ethdb.Database) error {
	v, _, err := stages.GetStageProgress(db, stages.Bodies)
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
	err = stages.SaveStageProgress(db, stages.Bodies, number, nil)
	if err != nil {
		return err
	}
	return nil
}

func GenerateHeaderIndexes(ctx context.Context, db ethdb.Database) error {
	var hash common.Hash
	var number uint64

	v, _, err := stages.GetStageProgress(db, HeaderNumber)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}

	if v == 0 {
		log.Info("Generate headers hash to number index")

		innerErr := etl.Transform("Torrent post-processing 1", db, dbutils.HeaderPrefix, dbutils.HeaderNumberPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			switch {
			case len(k) == 40:
				return next(k, common.CopyBytes(k[8:]), common.CopyBytes(k[:8]))
			case len(k) == 8:
				return next(k, common.CopyBytes(v[:common.HashLength]), k)
			default:
				return nil
			}
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: ctx.Done(),
			OnLoadCommit: func(db ethdb.Putter, key []byte, isDone bool) error {
				if !isDone {
					return nil
				}
				return stages.SaveStageProgress(db, HeaderNumber, 1, nil)
			},
		})
		if innerErr != nil {
			return innerErr
		}
	}

	v, _, err = stages.GetStageProgress(db, HeaderCanonical)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}
	if v == 0 {
		h := rawdb.ReadHeaderByNumber(db, 0)
		td := h.Difficulty

		log.Info("Generate TD index & canonical")
		err = etl.Transform("Torrent post-processing 2", db, dbutils.HeaderPrefix, dbutils.HeaderPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 8+common.HashLength && len(k) != 8 {
				return nil
			}
			header := &types.Header{}
			if len(k) == 8 {
				v = v[common.HashLength:]
			}
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

			return next(k, dbutils.HeaderTDKey(header.Number.Uint64(), header.Hash()), tdBytes)
		}, etl.IdentityLoadFunc, etl.TransformArgs{
			Quit: ctx.Done(),
			OnLoadCommit: func(db ethdb.Putter, key []byte, isDone bool) error {
				if !isDone {
					return nil
				}

				rawdb.WriteHeadHeaderHash(db, hash)
				rawdb.WriteHeaderNumber(db, hash, number)
				err = stages.SaveStageProgress(db, stages.Headers, number, nil)
				if err != nil {
					return err
				}
				err = stages.SaveStageProgress(db, stages.BlockHashes, number, nil)
				if err != nil {
					return err
				}
				rawdb.WriteHeadBlockHash(db, hash)
				return stages.SaveStageProgress(db, HeaderCanonical, number, nil)
			},
		})
		if err != nil {
			return err
		}
		log.Info("Last processed block", "num", number, "hash", hash.String())
	}

	return nil
}
