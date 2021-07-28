package verify

import (
	"context"
	"errors"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbx"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/rlp"
)

func HeadersSnapshot(logger log.Logger, snapshotPath string) error {
	snKV := mdbx.NewMDBX(logger).Path(snapshotPath).Readonly().WithBucketsConfig(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.Headers: kv.TableConfigItem{},
		}
	}).MustOpen()
	var prevHeader *types.Header
	err := snKV.View(context.Background(), func(tx kv.Tx) error {
		c, err := tx.Cursor(kv.Headers)
		if err != nil {
			return err
		}
		k, v, innerErr := c.First()
		for {
			if len(k) == 0 && len(v) == 0 {
				break
			}
			if innerErr != nil {
				return innerErr
			}

			header := new(types.Header)
			innerErr := rlp.DecodeBytes(v, header)
			if innerErr != nil {
				return innerErr
			}

			if prevHeader != nil {
				if prevHeader.Number.Uint64()+1 != header.Number.Uint64() {
					log.Error("invalid header number", "p", prevHeader.Number.Uint64(), "c", header.Number.Uint64())
					return errors.New("invalid header number")
				}
				if prevHeader.Hash() != header.ParentHash {
					log.Error("invalid parent hash", "p", prevHeader.Hash(), "c", header.ParentHash)
					return errors.New("invalid parent hash")
				}
			}
			k, v, innerErr = c.Next() //nolint
			prevHeader = header
		}
		return nil
	})
	return err
}
