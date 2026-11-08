package state

import (
	"context"
	"os"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment"
)

// PrefetchBranchRefs reads, in page batches, the account and storage file pages that the shortened keys of these
// commitment branches (as stored in files) point to. Probe for a second-stage trie prefetch.
func (at *AggregatorRoTx) PrefetchBranchRefs(ctx context.Context, prefixes [][]byte) (branches, pages int, err error) {
	cd, acc, sto := at.d[kv.CommitmentDomain], at.d[kv.AccountsDomain], at.d[kv.StorageDomain]
	pg := uintptr(os.Getpagesize())
	seen := map[uintptr]struct{}{}
	var list []uintptr
	add := func(r *seg.Reader, key []byte) {
		off := DecodeReferenceKey(key)
		a, ok := r.PageAddr(off)
		if !ok {
			return
		}
		for _, p := range []uintptr{a, a + pg} { // a word can run into the next page
			if _, dup := seen[p]; !dup {
				seen[p] = struct{}{}
				list = append(list, p)
			}
		}
	}
	readers := map[*seg.Decompressor]*seg.Reader{}
	reader := func(dt *DomainRoTx, f *FilesItem) *seg.Reader {
		r, ok := readers[f.decompressor]
		if !ok {
			r = dt.dataReader(f.decompressor)
			readers[f.decompressor] = r
		}
		return r
	}
	for _, pfx := range prefixes {
		if ctx.Err() != nil {
			break
		}
		v, found, fs, fe, gerr := cd.getLatestFromFiles(pfx, nil, kv.NoStepBound)
		if gerr != nil || !found || len(v) == 0 {
			continue
		}
		ver, _ := at.commitmentFileVersionByRange(fs, fe)
		if !CommitmentBranchReferenced(ver, at.StepSize(), fs, fe) {
			continue
		}
		accF, err1 := acc.lookupVisibleFileByRange(fs, fe)
		stoF, err2 := sto.lookupVisibleFileByRange(fs, fe)
		if err1 != nil || err2 != nil || accF == nil || stoF == nil {
			continue
		}
		ar, sr := reader(acc, accF), reader(sto, stoF)
		branches++
		_, _ = commitment.BranchData(v).ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			switch {
			case isStorage && len(key) != length.Addr+length.Hash:
				add(sr, key)
			case !isStorage && len(key) != length.Addr:
				add(ar, key)
			}
			return nil, nil
		})
	}
	return branches, len(list), mmap.ReadPages(ctx, list)
}
