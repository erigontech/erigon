package torrent

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
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
	h := rawdb.ReadHeaderByNumber(db, 0)
	td := h.Difficulty
	var hash common.Hash
	var number uint64

	v, _, err := stages.GetStageProgress(db, HeaderNumber)
	if err != nil {
		return err
	}

	if v == 0 {
		log.Info("Generate headers hash to number index")
		headHashBytes, innerErr := db.Get(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadHash))
		if innerErr != nil {
			return innerErr
		}

		headNumberBytes, innerErr := db.Get(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadNumber))
		if innerErr != nil {
			return innerErr
		}
		headNumber := big.NewInt(0).SetBytes(headNumberBytes).Uint64()
		headHash := common.BytesToHash(headHashBytes)

		innerErr = etl.Transform(db, dbutils.HeaderPrefix, dbutils.HeaderNumberPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
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
				return stages.SaveStageProgress(db, HeaderNumber, 1, nil)
			},
			ExtractEndKey: dbutils.HeaderKey(headNumber, headHash),
		})
		if innerErr != nil {
			return innerErr
		}
	}

	v, _, err = stages.GetStageProgress(db, HeaderCanonical)
	if err != nil {
		return err
	}
	if v == 0 {
		log.Info("Generate TD index & canonical")
		err = etl.Transform(db, dbutils.HeaderPrefix, dbutils.HeaderPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
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
		fmt.Println("Last processed block", number, hash.String())
	}

	return nil
}

func BuildInfoBytesForLMDBSnapshot(root string) (metainfo.Info, error) {
	path := root + "/" + "data.mdb"
	fi, err := os.Stat(path)
	if err != nil {
		return metainfo.Info{}, err
	}
	relPath, err := filepath.Rel(root, path)
	if err != nil {
		return metainfo.Info{}, fmt.Errorf("error getting relative path: %s", err)
	}

	info := metainfo.Info{
		Name:        filepath.Base(root),
		PieceLength: DefaultChunkSize,
		Length:      fi.Size(),
		Files: []metainfo.FileInfo{
			{
				Length:   fi.Size(),
				Path:     []string{relPath},
				PathUTF8: nil,
			},
		},
	}

	err = info.GeneratePieces(func(fi metainfo.FileInfo) (io.ReadCloser, error) {
		fmt.Println("info.GeneratePieces", filepath.Join(root, strings.Join(fi.Path, string(filepath.Separator))))
		return os.Open(filepath.Join(root, strings.Join(fi.Path, string(filepath.Separator))))
	})
	if err != nil {
		err = fmt.Errorf("error generating pieces: %s", err)
		return metainfo.Info{}, err
	}
	return info, nil
}
