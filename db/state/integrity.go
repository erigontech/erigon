package state

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
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
func (at *AggregatorRoTx) IntegrityInvertedIndexKey(domain kv.Domain, k []byte) error {
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
		err := at.d[kv.StorageDomain].ht.iit.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
		if err != nil {
			return err
		}
	case kv.CodeHistoryIdx:
		err := at.d[kv.CodeDomain].ht.iit.IntegrityInvertedIndexAllValuesAreInRange(ctx, failFast, fromStep)
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
	hi, lo := dt.ht.iit.hashKey(k)
	for i, f := range slices.Backward(dt.files) {
		_, ok, _, err := dt.getLatestFromFile(i, k, nil, hi, lo)
		if err != nil {
			return res, err
		}
		if ok {
			res = append(res, f.src.decompressor.FileName())
		}
	}
	return res, nil
}
func (dt *DomainRoTx) IntegrityKey(k []byte) error {
	for _, f := range dt.ht.iit.files {
		item := f.src
		if item == nil || item.decompressor == nil {
			continue
		}
		accessor := item.index
		needClose := false
		if accessor == nil {
			fPath, _, _, err := version.FindFilesWithVersionsByPattern(dt.d.efAccessorFilePathMask(item.StepRange(dt.stepSize)))
			if err != nil {
				panic(err)
			}

			exists, err := dir.FileExist(fPath)
			if err != nil {
				_, fName := filepath.Split(fPath)
				dt.d.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
				continue
			}
			if exists {
				var err error
				accessor, err = dt.d.openHashMapAccessor(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					dt.d.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
					continue
				}
			} else {
				continue
			}
			needClose = true
		}

		reader := accessor.Reader()
		offset, ok := reader.Lookup(k)
		reader.Close()
		if !ok {
			if needClose {
				accessor.Close()
			}
			continue
		}
		g := item.decompressor.MakeGetter()
		g.Reset(offset)
		key, _ := g.NextUncompressed()
		if !bytes.Equal(k, key) {
			if needClose {
				accessor.Close()
			}
			continue
		}
		eliasVal, _ := g.NextUncompressed()
		r := multiencseq.ReadMultiEncSeq(item.startTxNum, eliasVal)
		last2 := uint64(0)
		if r.Count() > 2 {
			last2 = r.Get(r.Count() - 2)
		}
		log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d", item.decompressor.FileName(), r.Min(), r.Max(), last2, stream.ToArrU64Must(r.Iterator(0))))
		if needClose {
			accessor.Close()
		}
	}
	return nil
}

func (iit *InvertedIndexRoTx) IntegrityInvertedIndexAllValuesAreInRange(ctx context.Context, failFast bool, fromStep uint64) error {
	fromTxNum := fromStep * iit.ii.stepSize
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(estimate.AlmostAllCPUs())

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	iterStep := func(item visibleFile) (err error) {
		defer func() {
			if r := recover(); r != nil {
				fileName := "not found"
				if item.src != nil && item.src.decompressor != nil {
					fileName = item.src.decompressor.FileName()
				}
				err = fmt.Errorf("panic in file: %s. Stack: %s", fileName, dbg.Stack())
			}
		}()
		item.src.decompressor.MadvSequential()
		defer item.src.decompressor.DisableReadAhead()

		g := item.src.decompressor.MakeGetter()
		g.Reset(0)

		i := 0
		var s multiencseq.SequenceReader

		for g.HasNext() {
			k, _ := g.NextUncompressed()
			_ = k

			encodedSeq, _ := g.NextUncompressed()
			s.Reset(item.startTxNum, encodedSeq)

			if s.Count() == 0 {
				continue
			}
			if s.Count() > 1 && s.Max() < s.Min() {
				err := fmt.Errorf("[integrity] .ef file has unsorted sequence: Max=%d < Min=%d, count=%d, %s, %x", s.Max(), s.Min(), s.Count(), g.FileName(), common.Shorten(k, 8))
				if failFast {
					return err
				} else {
					log.Warn(err.Error())
				}
			}
			if item.startTxNum > s.Min() {
				err := fmt.Errorf("[integrity] .ef file has foreign txNum: %d > %d, %s, %x", item.startTxNum, s.Min(), g.FileName(), common.Shorten(k, 8))
				if failFast {
					return err
				} else {
					log.Warn(err.Error())
				}
			}
			if item.endTxNum < s.Max() {
				err := fmt.Errorf("[integrity] .ef file has foreign txNum: %d < %d, %s, %x", item.endTxNum, s.Max(), g.FileName(), common.Shorten(k, 8))
				if failFast {
					return err
				} else {
					log.Warn(err.Error())
				}
			}
			i++

			if i%1000 == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[integrity] InvertedIndex: %s, prefix=%x", g.FileName(), common.Shorten(k, 8)))
				default:
				}
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
		g.Go(func() error {
			return iterStep(item)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// IntegrityHistoryValueIndex re-derives every .v offset the way buildVI does and
// compares it with what the .vi answers. It also covers a v1 .vi, which is asked
// the same question by txNum+key.
func (at *AggregatorRoTx) IntegrityHistoryValueIndex(ctx context.Context, domain kv.Domain, failFast bool, fromStep uint64) error {
	return at.d[domain].ht.IntegrityHistoryValueIndex(ctx, failFast, fromStep)
}

func (ht *HistoryRoTx) IntegrityHistoryValueIndex(ctx context.Context, failFast bool, fromStep uint64) error {
	fromTxNum := fromStep * ht.h.stepSize
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(estimate.AlmostAllCPUs())

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for _, efItem := range ht.iit.files {
		if efItem.src.decompressor == nil || efItem.endTxNum <= fromTxNum {
			continue
		}
		g.Go(func() error {
			err := ht.checkValueIndexOfFile(ctx, efItem, logEvery)
			if err != nil && !failFast {
				log.Warn(err.Error())
				return nil
			}
			return err
		})
	}
	return g.Wait()
}

func (ht *HistoryRoTx) checkValueIndexOfFile(ctx context.Context, efItem visibleFile, logEvery *time.Ticker) error {
	vItem, ok := ht.pairedFile(efItem)
	if !ok {
		return fmt.Errorf("[integrity] no .v file paired with %s", efItem.src.decompressor.FileName())
	}
	vi := vItem.src.vi
	if vi.Empty() {
		return nil
	}
	hist := vItem.src.decompressor
	pageSize := uint64(hist.CompressedPageValuesCount())
	if hist.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
		pageSize = uint64(ht.h.HistoryValuesOnCompressedPage)
	}
	if pageSize == 0 {
		pageSize = 1
	}

	efReader := ht.iit.dataReader(efItem.src.decompressor)
	efReader.Reset(0)
	histReader := ht.dataReader(hist)
	histReader.Reset(0)

	var key, encodedSeq []byte
	var seq multiencseq.SequenceReader
	var it multiencseq.SequenceIterator
	var valOffset, value uint64
	for keyOrdinal := uint64(0); efReader.HasNext(); keyOrdinal++ {
		key, _ = efReader.Next(key[:0])
		encodedSeq, _ = efReader.Next(encodedSeq[:0])
		seq.Reset(efItem.startTxNum, encodedSeq)
		it.Reset(&seq, 0)
		for rank := uint64(0); it.HasNext(); rank++ {
			txNum, err := it.Next()
			if err != nil {
				return err
			}
			got, ok := vi.Lookup(keyOrdinal, rank, txNum, key)
			if !ok {
				return fmt.Errorf("[integrity] %s: no offset for key %x at txNum %d (run %d, item %d)",
					vi.FilePath(), common.Shorten(key, 8), txNum, keyOrdinal, rank)
			}
			if got != valOffset {
				return fmt.Errorf("[integrity] %s: key %x at txNum %d resolves to offset %d, .v holds it at %d",
					vi.FilePath(), common.Shorten(key, 8), txNum, got, valOffset)
			}
			value++
			if value%pageSize == 0 {
				valOffset, _ = histReader.Skip()
			}
		}
		if keyOrdinal%1024 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[integrity] HistoryVi: %s, prefix=%x", vi.FilePath(), common.Shorten(key, 8)))
			default:
			}
		}
	}
	return nil
}
