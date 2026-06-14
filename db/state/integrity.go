package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
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
	hi, lo := dt.ht.iit.hashKey(k)
	for i := len(dt.files) - 1; i >= 0; i-- {
		_, ok, _, err := dt.getLatestFromFile(i, k, hi, lo)
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
	g := &errgroup.Group{}
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

// IntegrityHistoryEfConsistency verifies that history .v files and inverted-index .ef files
// have their entries in the same order. A mismatch means the .vi accessor maps some
// historyKeys to wrong pages, causing HistorySeek to silently return wrong data.
func (at *AggregatorRoTx) IntegrityHistoryEfConsistency(ctx context.Context, failFast bool, fromStep uint64) error {
	g := &errgroup.Group{}
	g.SetLimit(estimate.AlmostAllCPUs())

	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain} {
		ht := at.d[domain].ht
		g.Go(func() error {
			return ht.IntegrityHistoryEfConsistency(ctx, failFast, fromStep)
		})
	}
	return g.Wait()
}

func (ht *HistoryRoTx) IntegrityHistoryEfConsistency(ctx context.Context, failFast bool, fromStep uint64) error {
	fromTxNum := fromStep * ht.stepSize
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for i, histFile := range ht.files {
		if histFile.endTxNum <= fromTxNum {
			continue
		}
		if histFile.src.decompressor == nil {
			continue
		}
		if i >= len(ht.iit.files) {
			continue
		}
		iiFile := ht.iit.files[i]
		if iiFile.src.decompressor == nil {
			continue
		}
		// Verify step ranges match
		if iiFile.startTxNum != histFile.startTxNum || iiFile.endTxNum != histFile.endTxNum {
			log.Warn(fmt.Sprintf("[integrity] HistoryEfConsistency: step range mismatch at index %d: hist=%d-%d, ef=%d-%d",
				i, histFile.startTxNum, histFile.endTxNum, iiFile.startTxNum, iiFile.endTxNum))
			continue
		}

		mismatches, err := ht.checkHistoryEfFile(ctx, histFile, iiFile, failFast, logEvery)
		if err != nil {
			return err
		}
		if mismatches > 0 {
			err := fmt.Errorf("[integrity] HistoryEfConsistency: %d mismatches in %s (steps %d-%d)",
				mismatches, histFile.src.decompressor.FileName(), histFile.startTxNum/ht.stepSize, histFile.endTxNum/ht.stepSize)
			if failFast {
				return err
			}
			log.Warn(err.Error())
		}
	}
	return nil
}

func (ht *HistoryRoTx) checkHistoryEfFile(ctx context.Context, histFile, iiFile visibleFile, failFast bool, logEvery *time.Ticker) (int, error) {
	histFile.src.decompressor.MadvSequential()
	defer histFile.src.decompressor.DisableReadAhead()
	iiFile.src.decompressor.MadvSequential()
	defer iiFile.src.decompressor.DisableReadAhead()

	pageSize := histFile.src.decompressor.CompressedPageValuesCount()
	if histFile.src.decompressor.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
		pageSize = ht.h.HistoryValuesOnCompressedPage
	}
	if pageSize == 0 {
		pageSize = 1
	}

	// EF reader
	efReader := ht.h.InvertedIndex.dataReader(iiFile.src.decompressor)
	// History reader (paged)
	histReader := ht.h.dataReader(histFile.src.decompressor)
	paged := seg.NewPagedReader(histReader, pageSize, true)
	paged.Reset(0)

	seq := &multiencseq.SequenceReader{}
	it := &multiencseq.SequenceIterator{}
	var keyBuf, valBuf, expectedKey []byte

	entryNum := 0
	mismatches := 0
	maxReport := 20

	for efReader.HasNext() {
		keyBuf, _ = efReader.Next(keyBuf[:0])
		valBuf, _ = efReader.Next(valBuf[:0])

		seq.Reset(iiFile.startTxNum, valBuf)
		it.Reset(seq, 0)

		for it.HasNext() {
			txNum, err := it.Next()
			if err != nil {
				return mismatches, fmt.Errorf("[integrity] HistoryEfConsistency: sequence decode error at entry %d in %s: %w",
					entryNum, iiFile.src.decompressor.FileName(), err)
			}

			if !paged.HasNext() {
				return mismatches, fmt.Errorf("[integrity] HistoryEfConsistency: .v file exhausted at entry %d in %s (ef expects more)",
					entryNum, histFile.src.decompressor.FileName())
			}

			actualKey, _, _, _ := paged.Next2(nil)

			expectedKey = expectedKey[:0]
			expectedKey = binary.BigEndian.AppendUint64(expectedKey, txNum)
			expectedKey = append(expectedKey, keyBuf...)

			if !bytes.Equal(expectedKey, actualKey) {
				mismatches++
				if mismatches <= maxReport {
					var vTxNum uint64
					var vKey []byte
					if len(actualKey) >= 8 {
						vTxNum = binary.BigEndian.Uint64(actualKey[:8])
					}
					if len(actualKey) > 8 {
						vKey = actualKey[8:]
					}
					log.Warn(fmt.Sprintf("[integrity] HistoryEfConsistency: mismatch entry=%d ef=(txNum=%d, key=%x) vs v=(txNum=%d, key=%x) file=%s",
						entryNum, txNum, common.Shorten(keyBuf, 8), vTxNum, common.Shorten(vKey, 8), histFile.src.decompressor.FileName()))
				}
			}

			entryNum++
			if entryNum%10_000_000 == 0 {
				select {
				case <-ctx.Done():
					return mismatches, ctx.Err()
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[integrity] HistoryEfConsistency: %s %dM entries, %d mismatches",
						histFile.src.decompressor.FileName(), entryNum/1_000_000, mismatches))
				default:
				}
			}
		}
	}

	log.Info(fmt.Sprintf("[integrity] HistoryEfConsistency: %s done, %d entries, %d mismatches",
		histFile.src.decompressor.FileName(), entryNum, mismatches))
	return mismatches, nil
}
