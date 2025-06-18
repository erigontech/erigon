package state

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/multiencseq"
)

// search key in all files of all domains and print file names
func (at *AggregatorRoTx) IntegrityKey(domain kv.Domain, k []byte) error {
	l, err := at.d[domain].IntegrityDomainFilesWithKey(k)
	if err != nil {
		return err
	}
	if len(l) > 0 {
		at.a.logger.Info("[dbg] found in", "files", l)
	}
	return nil
}
func (at *AggregatorRoTx) IntegirtyInvertedIndexKey(domain kv.Domain, k []byte) error {
	return at.d[domain].IntegrityKey(k)
}

func (at *AggregatorRoTx) IntegrityInvertedIndexAllValuesAreInRange(ctx context.Context, name kv.InvertedIdx, failFast bool, fromStep uint64) error {
	switch name {
	case kv.AccountsHistoryIdx:
		err := at.d[kv.AccountsDomain].ht.iit.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
		if err != nil {
			return err
		}
	case kv.StorageHistoryIdx:
		err := at.d[kv.CodeDomain].ht.iit.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
		if err != nil {
			return err
		}
	case kv.CodeHistoryIdx:
		err := at.d[kv.StorageDomain].ht.iit.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
		if err != nil {
			return err
		}
	case kv.CommitmentHistoryIdx:
		err := at.d[kv.CommitmentDomain].ht.iit.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
		if err != nil {
			return err
		}
	case kv.ReceiptHistoryIdx:
		err := at.d[kv.ReceiptDomain].ht.iit.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
		if err != nil {
			return err
		}
	case kv.RCacheHistoryIdx:
		err := at.d[kv.RCacheDomain].ht.iit.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
		if err != nil {
			return err
		}
	default:
		// check the ii
		if v := at.searchII(name); v != nil {
			return v.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
		}
		panic(fmt.Sprintf("unexpected: %s", name))
	}
	return nil
}

func (dt *DomainRoTx) IntegrityDomainFilesWithKey(k []byte) (res []string, err error) {
	for i := len(dt.files) - 1; i >= 0; i-- {
		_, ok, _, err := dt.getLatestFromFile(i, k)
		if err != nil {
			return res, err
		}
		if ok {
			res = append(res, dt.files[i].src.decompressor.FileName())
		}
	}
	return res, nil
}
func (dt *DomainRoTx) IntegrityKey(k []byte) error {
	dt.ht.iit.ii.dirtyFiles.Walk(func(items []*FilesItem) bool {
		for _, item := range items {
			if item.decompressor == nil {
				continue
			}
			accessor := item.index
			if accessor == nil {
				fPath := dt.d.efAccessorFilePath(item.startTxNum/dt.aggStep, item.endTxNum/dt.aggStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					dt.d.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
					continue
				}
				if exists {
					var err error
					accessor, err = recsplit.OpenIndex(fPath)
					if err != nil {
						_, fName := filepath.Split(fPath)
						dt.d.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
						continue
					}
					defer accessor.Close()
				} else {
					continue
				}
			}

			offset, ok := accessor.GetReaderFromPool().Lookup(k)
			if !ok {
				continue
			}
			g := item.decompressor.MakeGetter()
			g.Reset(offset)
			key, _ := g.NextUncompressed()
			if !bytes.Equal(k, key) {
				continue
			}
			eliasVal, _ := g.NextUncompressed()
			r := multiencseq.ReadMultiEncSeq(item.startTxNum, eliasVal)
			last2 := uint64(0)
			if r.Count() > 2 {
				last2 = r.Get(r.Count() - 2)
			}
			log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d", item.decompressor.FileName(), r.Min(), r.Max(), last2, stream.ToArrU64Must(r.Iterator(0))))
		}
		return true
	})
	return nil
}

func (iit *InvertedIndexRoTx) IntegrityInvertedIndexAllValuesAreInRange(ctx context.Context, failFast bool, fromStep uint64) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	fromTxNum := fromStep * iit.ii.aggregationStep
	iterStep := func(item visibleFile) error {
		g := item.src.decompressor.MakeGetter()
		g.Reset(0)

		for g.HasNext() {
			k, _ := g.NextUncompressed()
			_ = k

			encodedSeq, _ := g.NextUncompressed()
			r := multiencseq.ReadMultiEncSeq(item.startTxNum, encodedSeq)
			if r.Count() == 0 {
				continue
			}
			if item.startTxNum > r.Min() {
				err := fmt.Errorf("[integrity] .ef file has foreign txNum: %d > %d, %s, %x", item.startTxNum, r.Min(), g.FileName(), common.Shorten(k, 8))
				if failFast {
					return err
				} else {
					log.Warn(err.Error())
				}
			}
			if item.endTxNum < r.Max() {
				err := fmt.Errorf("[integrity] .ef file has foreign txNum: %d < %d, %s, %x", item.endTxNum, r.Max(), g.FileName(), common.Shorten(k, 8))
				if failFast {
					return err
				} else {
					log.Warn(err.Error())
				}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[integrity] InvertedIndex: %s, prefix=%x", g.FileName(), common.Shorten(k, 8)))
			default:
			}
		}
		return nil
	}

	for _, item := range iit.files {
		if item.src.decompressor == nil {
			continue
		}
		if item.endTxNum <= fromTxNum {
			continue
		}
		if err := iterStep(item); err != nil {
			return err
		}
		//log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d", item.src.decompressor.FileName(), ef.Min(), ef.Max(), last2, stream.ToArrU64Must(ef.Iterator())))
	}
	return nil
}
