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
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
)

// search key in all files of all domains and print file names
func (ac *AggregatorRoTx) DebugKey(domain kv.Domain, k []byte) error {
	l, err := ac.d[domain].DebugKVFilesWithKey(k)
	if err != nil {
		return err
	}
	if len(l) > 0 {
		ac.a.logger.Info("[dbg] found in", "files", l)
	}
	return nil
}
func (ac *AggregatorRoTx) DebugEFKey(domain kv.Domain, k []byte) error {
	return ac.d[domain].DebugEFKey(k)
}

func (ac *AggregatorRoTx) DebugInvertedIndexOfDomainAllValuesAreInRange(ctx context.Context, name kv.Domain, failFast bool, fromStep uint64) error {
	return ac.d[name].ht.iit.DebugEFAllValuesAreInRange(ctx, failFast, fromStep)
}
func (ac *AggregatorRoTx) DebugInvertedIndexOfDomainCount(ctx context.Context, name kv.Domain, failFast bool, k []byte, fromStep uint64) (int, error) {
	return ac.d[name].ht.iit.DebugInvertedIndexOfDomainCount(ctx, failFast, k, fromStep)
}

func (ac *AggregatorRoTx) DebugInvertedIndexAllValuesAreInRange(ctx context.Context, name kv.InvertedIdxPos, failFast bool, fromStep uint64) error {
	return ac.iis[name].DebugEFAllValuesAreInRange(ctx, failFast, fromStep)
}

func (iit *InvertedIndexRoTx) DebugEFAllValuesAreInRange(ctx context.Context, failFast bool, fromStep uint64) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	fromTxNum := fromStep * iit.ii.aggregationStep
	iterStep := func(item visibleFile) error {
		g := item.src.decompressor.MakeGetter()
		g.Reset(0)
		defer item.src.decompressor.EnableReadAhead().DisableReadAhead()

		for g.HasNext() {
			k, _ := g.NextUncompressed()
			_ = k
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			if ef.Count() == 0 {
				continue
			}
			if item.startTxNum > ef.Min() {
				err := fmt.Errorf("[integrity] .ef file has foreign txNum: %d > %d, %s, %x", item.startTxNum, ef.Min(), g.FileName(), common.Shorten(k, 8))
				if failFast {
					return err
				} else {
					log.Warn(err.Error())
				}
			}
			if item.endTxNum < ef.Max() {
				err := fmt.Errorf("[integrity] .ef file has foreign txNum: %d < %d, %s, %x", item.endTxNum, ef.Max(), g.FileName(), common.Shorten(k, 8))
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
		//log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d\n", item.src.decompressor.FileName(), ef.Min(), ef.Max(), last2, stream.ToArrU64Must(ef.Iterator())))
	}
	return nil
}

func (iit *InvertedIndexRoTx) countEvents(key []byte, txNum uint64) (cnt int, err error) {
	if len(iit.files) == 0 {
		return 0, nil
	}
	if iit.files[0].isAfter(txNum) {
		return 0, fmt.Errorf("seek with txNum=%d but data before txNum=%d is not available", txNum, iit.files[0].startTxNum)
	}
	if iit.files[len(iit.files)-1].isBefore(txNum) {
		return 0, nil
	}

	hi, lo := iit.hashKey(key)
	for i := 0; i < len(iit.files); i++ {
		if iit.files[i].isBefore(txNum) {
			continue
		}
		offset, ok := iit.statelessIdxReader(i).TwoLayerLookupByHash(hi, lo)
		if !ok {
			continue
		}

		g := iit.statelessGetter(i)
		g.Reset(offset)
		k, _ := g.Next(nil)
		if !bytes.Equal(k, key) {
			continue
		}
		eliasVal, _ := g.Next(nil)
		return int(eliasfano32.Count(eliasVal)), nil
	}
	return 0, nil
}

func (iit *InvertedIndexRoTx) DebugInvertedIndexOfDomainCount(ctx context.Context, failFast bool, k []byte, fromStep uint64) (int, error) {
	fromTxNum := fromStep * iit.ii.aggregationStep
	return iit.countEvents(k, fromTxNum)
}

func (dt *DomainRoTx) DebugKVFilesWithKey(k []byte) (res []string, err error) {
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
func (dt *DomainRoTx) DebugEFKey(k []byte) error {
	dt.ht.iit.ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor == nil {
				continue
			}
			accessor := item.index
			if accessor == nil {
				fPath := dt.d.efAccessorFilePath(item.startTxNum/dt.d.aggregationStep, item.endTxNum/dt.d.aggregationStep)
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
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)

			last2 := uint64(0)
			if ef.Count() > 2 {
				last2 = ef.Get(ef.Count() - 2)
			}
			log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d\n", item.decompressor.FileName(), ef.Min(), ef.Max(), last2, stream.ToArrU64Must(ef.Iterator())))
		}
		return true
	})
	return nil
}
