package commitment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// if nibble set is -1 then subtrie is not mounted to the nibble, but limited by depth: eg do not fold mounted trie above depth 63
func (hph *HexPatriciaHashed) mountTo(root *HexPatriciaHashed, nibble int) {
	hph.Reset()

	hph.root = root.root
	// hph.rootPresent = !hph.root.IsEmpty()
	// hph.rootPresent = false

	hph.activeRows = root.activeRows
	hph.currentKeyLen = root.currentKeyLen
	copy(hph.currentKey[:], root.currentKey[:])
	copy(hph.depths[:], root.depths[:])
	copy(hph.branchBefore[:], root.branchBefore[:])
	copy(hph.touchMap[:], root.touchMap[:])
	copy(hph.afterMap[:], root.afterMap[:])
	copy(hph.depthsToTxNum[:], root.depthsToTxNum[:])

	hph.mountedNib = nibble
	hph.mounted = true
	for row := 0; row <= hph.activeRows; row++ {
		for nib := 0; nib < len(hph.grid[row]); nib++ {
			hph.grid[row][nib] = root.grid[row][nib]
		}
	}
}

type ConcurrentPatriciaHashed struct {
	root       *HexPatriciaHashed
	rootMu     sync.Mutex
	mounts     [16]*HexPatriciaHashed
	ctxClosers [16]func()
}

// Subtrie inherits root state, address length
func NewConcurrentPatriciaHashed(root *HexPatriciaHashed, ctx PatriciaContext) *ConcurrentPatriciaHashed {
	p := &ConcurrentPatriciaHashed{root: root}

	for i := range p.mounts {
		p.mounts[i] = p.root.SpawnSubTrie(ctx, i)
	}
	return p
}

func (p *ConcurrentPatriciaHashed) RootTrie() *HexPatriciaHashed {
	return p.root
}

func (p *ConcurrentPatriciaHashed) foldNibble(ctx context.Context, nib int) error {
	c, err := p.mounts[nib].foldMounted(ctx, nib)
	if err != nil {
		return err
	}

	p.rootMu.Lock()
	defer p.rootMu.Unlock()

	// fmt.Printf("mounted %02x => %s\n", prevByte, c.String())
	if c.extLen > 0 { // trim first byte (2 nibbles) from extension, if any, since it's also a nibble in that row
		c.extLen--
		copy(c.extension[:], c.extension[1:])
		c.hashedExtLen -= 2
		copy(c.hashedExtension[:], c.hashedExtension[2:])
	}

	// propagate changes to top row
	p.root.touchMap[0] |= uint16(1) << nib
	if !c.IsEmpty() {
		p.root.afterMap[0] |= uint16(1) << nib
	} else {
		p.root.afterMap[0] &^= uint16(1) << nib
	}
	p.root.depths[0] = 1
	p.root.grid[0][nib] = c

	subtrie := p.mounts[nib]
	subtrie.Reset()

	// clean up subtrie
	subtrie.currentKeyLen = 0
	subtrie.activeRows = 0
	for ri := 0; ri < len(p.mounts[nib].grid); ri++ {
		subtrie.currentKey[ri] = 0
		subtrie.depths[ri] = 0
		subtrie.touchMap[ri] = 0
		subtrie.afterMap[ri] = 0
		subtrie.depthsToTxNum[ri] = 0
		subtrie.branchBefore[ri] = false

		for ci := 0; ci < len(subtrie.grid[ri]); ci++ {
			subtrie.grid[ri][ci].reset()
		}
	}

	return nil
}

func (p *ConcurrentPatriciaHashed) unfoldRoot(ctx context.Context, ctxFactory TrieContextFactory) error {
	if p.root.trace {
		fmt.Printf("=============ROOT unfold============\n")
	}
	// if p.root.rootPresent && p.root.root.hashedExtLen == 0 { // if root has no extension, we have to unfold
	zero := []byte{0}
	for unfolding := p.root.needUnfolding(zero); unfolding > 0; unfolding = p.root.needUnfolding(zero) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.root.unfold(zero, unfolding); err != nil {
			return fmt.Errorf("unfold: %w", err)
		}
	}
	// }

	if p.root.trace {
		fmt.Printf("=========END=ROOT unfold============\n")
	}

	for i := range p.mounts {
		if p.mounts[i] == nil {
			panic(fmt.Sprintf("nibble %x is nil", i))
		}
		p.mounts[i].mountTo(p.root, i)
		mountCtx, mountCtxClose := ctxFactory()
		p.mounts[i].ctx = mountCtx
		p.ctxClosers[i] = mountCtxClose
	}
	return nil
}

func (p *ConcurrentPatriciaHashed) Close() {
	for i := range p.mounts {
		p.mounts[i].Reset()
		if p.ctxClosers[i] != nil {
			p.ctxClosers[i]()
			p.ctxClosers[i] = nil
		}
	}
}

func (p *ConcurrentPatriciaHashed) SetTrace(b bool) {
	p.root.SetTrace(b)
	for i := range p.mounts {
		p.mounts[i].SetTrace(b)
	}
}
func (p *ConcurrentPatriciaHashed) SetTraceDomain(b bool) {
	p.root.SetTraceDomain(b)
	for i := range p.mounts {
		p.mounts[i].SetTraceDomain(b)
	}
}
func (p *ConcurrentPatriciaHashed) EnableWarmupCache(b bool) {
	p.root.EnableWarmupCache(b)
	for i := range p.mounts {
		p.mounts[i].EnableWarmupCache(b)
	}
}
func (p *ConcurrentPatriciaHashed) GetCapture(truncate bool) []string {
	capture := p.root.GetCapture(truncate)
	if truncate {
		for i := range p.mounts {
			p.mounts[i].SetCapture(nil)
		}
	}
	return capture
}

func (p *ConcurrentPatriciaHashed) SetCapture(capture []string) {
	p.root.SetCapture(capture)
	for i := range p.mounts {
		p.mounts[i].SetCapture(capture)
	}
}

func (p *ConcurrentPatriciaHashed) EnableCsvMetrics(filePathPrefix string) {
	p.root.EnableCsvMetrics(filePathPrefix)
	for i := range p.mounts {
		p.mounts[i].EnableCsvMetrics(filePathPrefix)
		p.mounts[i].metrics = p.root.metrics
	}
}

// pass -1 to enable trace just for root trie
func (p *ConcurrentPatriciaHashed) SetParticularTrace(b bool, n int) {
	p.root.SetTrace(b)
	if n < len(p.mounts) && n >= 0 {
		p.mounts[n].SetTrace(b)
	}
}

func (t *Updates) ParallelHashSort(ctx context.Context, pph *ConcurrentPatriciaHashed, trieCtxFactory TrieContextFactory) ([]byte, error) {
	if t.mode != ModeDirect {
		return nil, errors.New("parallel hashsort for indirect mode is not supported")
	}
	if !t.sortPerNibble {
		return nil, errors.New("sortPerNibble disabled")
	}

	if err := pph.unfoldRoot(ctx, trieCtxFactory); err != nil {
		return nil, err
	}

	clear(t.keys)

	// Use a derived context for the errgroup goroutines only.
	// The original ctx is preserved for the root fold loop below, because
	// errgroup cancels the derived context after g.Wait() returns, and we
	// must not see a spurious context.Canceled on the subsequent root fold.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)

	for n := 0; n < len(t.nibbles); n++ {
		nib := t.nibbles[n]
		phnib := pph.mounts[n]
		ni := n

		g.Go(func() error {
			// close the temporary context provisioned by unfoldRoot before replacing it
			if pph.ctxClosers[ni] != nil {
				pph.ctxClosers[ni]()
				pph.ctxClosers[ni] = nil
			}
			trieCtx, trieCtxClose := trieCtxFactory()
			defer trieCtxClose()
			phnib.ResetContext(trieCtx)

			// Phase 2a — storage-level warmup-only fanout. When the threshold
			// is configured (>0) and a single account dominates this subtrie's
			// updates with >= threshold storage entries, we spawn 16 inner
			// goroutines that each `followAndUpdate` a CLONE subtrie over
			// 1/16 of the storage entries (split on keccak256(slot)[0]) using
			// independent MDBX RoTxs. The clones are discarded; their only
			// purpose is to populate the process-wide MDBX OS page cache so
			// the subsequent canonical pass on `phnib` reads warm pages.
			//
			// The per-HPH cellHash cache is per-instance and discarded with
			// the clone, so CPU-side hash work remains serial. Only I/O is
			// parallelised. The full mount-at-depth-64 fanout (Phase 2b)
			// adds CPU parallelism on top — see
			// docs/plans/storage-parallel-trie-fanout.md.
			detectThreshold := dbg.StorageParallelTrieThreshold

			// Buffer entries: we may need to iterate twice (once for warmup
			// clones, once for the canonical pass). 4200 entries × ~64 bytes
			// is ~270 KB — fine to keep in memory.
			type kvCopy struct{ hashedKey, plainKey []byte }
			var entries []kvCopy
			var storageBuckets [16][]kvCopy
			var (
				detectAccount    [32]byte
				detectCount      int
				detectAccountSet bool
				detectMixed      bool
			)
			cnt := 0
			err := nib.Load(nil, "", func(hashedKey, plainKey []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
				cnt++
				// etl callback reuses the underlying byte slices; copy before
				// keeping references across iterations.
				hk := append([]byte(nil), hashedKey...)
				pk := append([]byte(nil), plainKey...)
				entries = append(entries, kvCopy{hk, pk})
				if detectThreshold > 0 && !detectMixed && len(hashedKey) > 32 {
					if !detectAccountSet {
						copy(detectAccount[:], hashedKey[:32])
						detectAccountSet = true
						detectCount = 1
					} else if bytes.Equal(detectAccount[:], hashedKey[:32]) {
						detectCount++
					} else {
						detectMixed = true
					}
					if !detectMixed {
						storageBuckets[hashedKey[32]>>4] = append(storageBuckets[hashedKey[32]>>4], kvCopy{hk, pk})
					}
				}
				return nil
			}, etl.TransformArgs{Quit: gctx.Done()})
			if err != nil {
				return err
			}
			if cnt == 0 {
				return nil
			}

			doWarmupFanout := detectThreshold > 0 && !detectMixed && detectAccountSet && detectCount >= detectThreshold
			if doWarmupFanout {
				warmupStart := time.Now()
				log.Info("[commitment] storage-parallel-trie warmup-fanout starting",
					"subtrie", fmt.Sprintf("%x", ni),
					"account_keccak_prefix", fmt.Sprintf("%x", detectAccount[:8]),
					"storage_updates", detectCount,
					"threshold", detectThreshold)
				innerG, innerGctx := errgroup.WithContext(gctx)
				innerG.SetLimit(16)
				for b := 0; b < 16; b++ {
					if len(storageBuckets[b]) == 0 {
						continue
					}
					bucket := storageBuckets[b]
					innerG.Go(func() error {
						// Independent MDBX RoTx per clone so reads run in
						// parallel.
						wctx, wctxClose := trieCtxFactory()
						defer wctxClose()
						clone := NewHexPatriciaHashed(phnib.accountKeyLen, wctx)
						defer clone.Release()
						clone.mountTo(pph.root, ni)
						for _, e := range bucket {
							if err := innerGctx.Err(); err != nil {
								return err
							}
							// Warmup-only: ignore errors; the canonical pass
							// on phnib will surface real errors.
							_ = clone.followAndUpdate(e.hashedKey, e.plainKey, nil)
						}
						return nil
					})
				}
				if werr := innerG.Wait(); werr != nil {
					log.Debug("[commitment] storage-parallel-trie warmup error",
						"subtrie", fmt.Sprintf("%x", ni), "err", werr)
				}
				populated := 0
				for _, b := range storageBuckets {
					if len(b) > 0 {
						populated++
					}
				}
				log.Info("[commitment] storage-parallel-trie warmup-fanout done",
					"subtrie", fmt.Sprintf("%x", ni),
					"buckets_populated", populated,
					"duration", time.Since(warmupStart))
			}

			// Canonical pass on the parent subtrie. After 2a's warmup, MDBX
			// page reads here are likely served from the OS page cache.
			for _, e := range entries {
				if phnib.trace {
					fmt.Printf("\n%x) plainKey [%x] hashedKey [%x] currentKey [%x]\n", ni, e.plainKey, e.hashedKey, phnib.currentKey[:phnib.currentKeyLen])
				}
				if err := phnib.followAndUpdate(e.hashedKey, e.plainKey, nil); err != nil {
					return fmt.Errorf("followAndUpdate[%x]: %w", ni, err)
				}
			}
			if pph.mounts[ni].trace {
				fmt.Printf("ConcurrentTrie: folding [%2x] keys %d maxDepth %d\n", ni, cnt, phnib.depths[0])
			}
			return pph.foldNibble(gctx, ni)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	if pph.root.trace {
		fmt.Printf("======= folding ROOT trie =========\n")
	}
	// TODO zero active rows could be a clue to some invalid cases
	if pph.root.activeRows == 0 {
		pph.root.activeRows = 1
	}

	for pph.root.activeRows > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := pph.root.fold(); err != nil {
			return nil, err
		}
	}
	rootHash, err := pph.root.RootHash()
	if err != nil {
		return nil, err
	}
	if pph.root.trace {
		fmt.Printf("======= folding root done =========\n")
	}
	// have to reset trie since we do not do any unfolding
	return rootHash, nil
}

// Computing commitment root hash. If possible, use parallel commitment and after evaluation decides, if it can be used next time
func (p *ConcurrentPatriciaHashed) Process(ctx context.Context, updates *Updates, logPrefix string, onProgress func(*CommitProgress), warmup WarmupConfig) (rootHash []byte, err error) {
	start := time.Now()
	wasConcurrent := updates.IsConcurrentCommitment()
	updatesCount := updates.Size()
	defer func() {
		log.Debug(
			"concurrent commitment processed",
			"dur", time.Since(start),
			"updates", common.PrettyCounter(updatesCount),
			"wasConcurrent", wasConcurrent,
		)
	}()
	p.root.metrics.Reset()
	p.root.metrics.updates.Store(updatesCount)
	if p.root.metrics.collectCommitmentMetrics {
		defer func() {
			p.root.metrics.TotalProcessingTimeInc(start)
			p.root.metrics.WriteToCSV()
		}()
	}
	switch updates.IsConcurrentCommitment() {
	case true:
		rootHash, err = updates.ParallelHashSort(ctx, p, warmup.CtxFactory)
		if err != nil {
			return nil, err
		}
	default:
		rootHash, err = p.root.Process(ctx, updates, logPrefix, onProgress, warmup)
		if err != nil {
			return nil, err
		}
	}
	nextConcurrent, err := p.CanDoConcurrentNext()
	if err != nil {
		return nil, err
	}
	updates.SetConcurrentCommitment(nextConcurrent)
	return rootHash, nil
}

func (p *ConcurrentPatriciaHashed) CanDoConcurrentNext() (bool, error) {
	if p.root.root.extLen == 0 {
		zeroPrefixBranch, _, err := p.root.ctx.Branch(nibbles.HexToCompact([]byte{0}))
		if err != nil {
			return false, fmt.Errorf("checking shortes prefix branch failed: %w", err)
		}
		if len(zeroPrefixBranch) > 4 { // tm+am+cells
			// if root has no extension and there is a branch of zero prefix, can use parallel commitment next time
			// fmt.Printf("use concurrent next\n")
			return true, nil
		}
		// fmt.Printf(" 00 [branch %x len %d]\n", zeroPrefixBranch, len(zeroPrefixBranch))
	}
	// fmt.Printf("use seq trie next [root extLen=%d][ext '%x']\n", p.root.root.extLen, p.root.root.extension[:p.root.root.extLen])
	return false, nil
}

// Variant returns commitment trie variant
func (p *ConcurrentPatriciaHashed) Variant() TrieVariant {
	return VariantConcurrentHexPatricia
}

// Reset Drops everything from the trie
func (p *ConcurrentPatriciaHashed) Reset() {
	p.root.Reset()
	for i := 0; i < len(p.mounts); i++ {
		p.mounts[i].Reset()
	}
}

func (p *ConcurrentPatriciaHashed) Release() {
	for i := range p.mounts {
		if p.ctxClosers[i] != nil {
			p.ctxClosers[i]()
			p.ctxClosers[i] = nil
		}
		p.mounts[i].Release()
		p.mounts[i] = nil
	}
	p.root.Release()
	p.root = nil
}

// Set context for state IO
func (p *ConcurrentPatriciaHashed) ResetContext(ctx PatriciaContext) {
	p.root.ctx = ctx
	for i := 0; i < len(p.mounts); i++ {
		p.mounts[i].ResetContext(ctx)
	}
}

func (p *ConcurrentPatriciaHashed) RootHash() (hash []byte, err error) {
	return p.root.RootHash()
}
