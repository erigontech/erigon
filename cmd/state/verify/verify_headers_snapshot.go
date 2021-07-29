package verify

import (
	"context"
	"errors"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

func HeadersSnapshot(logger log.Logger, snapshotPath string) error {
	snKV := mdbx.NewMDBX(logger).Path(snapshotPath).Readonly().WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.Headers: kv.TableCfgItem{},
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
