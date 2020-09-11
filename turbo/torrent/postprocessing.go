package torrent

import (
	"context"
	"fmt"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
)


func GenerateHeaderIndexes(ctx context.Context, db ethdb.Database) error {
	td := big.NewInt(0)
	var hash common.Hash
	var number uint64

	err := etl.Transform(db, dbutils.HeaderPrefix,dbutils.HeaderNumberPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		if len(k)!=8+common.HashLength {
			return nil
		}
		return next(k, common.CopyBytes(k[8:]), common.CopyBytes(k[:8]))
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:              ctx.Done(),
	})
	if err!=nil {
		return err
	}

	err = etl.Transform(db, dbutils.HeaderPrefix,dbutils.HeaderPrefix, os.TempDir(), func(k []byte, v []byte, next etl.ExtractNextFunc) error {
		if len(k)!=8+common.HashLength {
			return nil
		}
		header:=&types.Header{}
		err:=rlp.DecodeBytes(v,header)
		if err!=nil {
			return err
		}
		number=header.Number.Uint64()
		hash=header.Hash()
		td = td.Add(td, header.Difficulty)
		tdBytes, innerErr := rlp.EncodeToBytes(td)
		if innerErr != nil {
			return innerErr
		}

		innerErr = next(k, dbutils.HeaderTDKey(header.Number.Uint64(), header.Hash()), tdBytes)
		if innerErr!=nil {
			return innerErr
		}

		//canonical
		return next(k, dbutils.HeaderHashKey(header.Number.Uint64()), header.Hash().Bytes())
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:              ctx.Done(),
		//OnLoadCommit: func(db ethdb.Putter, key []byte, isDone bool) error {
		//
		//},
	})
	if err!=nil {
		return err
	}
	rawdb.WriteHeadBlockHash(db, hash)
	rawdb.WriteHeadHeaderHash(db, hash)
	fmt.Println("Last processed block", number, hash.String())

	return nil
}





func BuildInfoBytesForLMDBSnapshot(root string) (metainfo.Info, error) {
	path:=root+"/"+"data.mdb"
	fi, err:=os.Stat(path)
	if err!=nil{
		return metainfo.Info{}, err
	}
	relPath, err := filepath.Rel(root, path)
	if err != nil {
		return metainfo.Info{}, fmt.Errorf("error getting relative path: %s", err)
	}

	info:=metainfo.Info{
		PieceLength: 16*1024,
		Files: []metainfo.FileInfo{
			{
				Length:   fi.Size(),
				Path:     []string{relPath},
				PathUTF8: nil,
			},
		},
	}

	err = info.GeneratePieces(func(fi metainfo.FileInfo) (io.ReadCloser, error) {
		return os.Open(filepath.Join(root, strings.Join(fi.Path, string(filepath.Separator))))
	})
	if err != nil {
		err = fmt.Errorf("error generating pieces: %s", err)
		return metainfo.Info{}, err
	}
	return info, nil
}
