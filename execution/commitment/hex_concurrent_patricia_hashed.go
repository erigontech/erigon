package commitment

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
)

// bufferedTrieContext wraps a PatriciaContext and buffers PutBranch writes
// so that concurrent goroutines in ParallelHashSort don't write to the shared
// MemBatch.  Branch reads check the local buffer first, falling through to
// the inner context on miss.  After all goroutines finish, the buffered writes
// are flushed sequentially through the root context.
type bufferedTrieContext struct {
	inner  PatriciaContext
	writes map[string][]byte // compact prefix → branch data
	prev   map[string][]byte // compact prefix → prev branch data
}

func newBufferedTrieContext(inner PatriciaContext) *bufferedTrieContext {
	return &bufferedTrieContext{
		inner:  inner,
		writes: make(map[string][]byte),
		prev:   make(map[string][]byte),
	}
}

func (b *bufferedTrieContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if data, ok := b.writes[string(prefix)]; ok {
		return data, 0, nil
	}
	return b.inner.Branch(prefix)
}

func (b *bufferedTrieContext) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	key := string(prefix)
	// Clone data and prevData: callers (CollectUpdate) already copy today, but
	// cloning here makes the buffer self-contained regardless of caller changes.
	b.writes[key] = slices.Clone(data)
	if _, exists := b.prev[key]; !exists {
		b.prev[key] = slices.Clone(prevData)
	}
	return nil
}

func (b *bufferedTrieContext) Account(plainKey []byte) (*Update, error) {
	return b.inner.Account(plainKey)
}

func (b *bufferedTrieContext) Storage(plainKey []byte) (*Update, error) {
	return b.inner.Storage(plainKey)
}

func (b *bufferedTrieContext) TxNum() uint64 {
	return b.inner.TxNum()
}

// flush writes all buffered PutBranch calls through the target context.
// Map iteration order is non-deterministic, but that is fine: each prefix
// is independent — PutBranch writes are keyed by distinct compact prefixes
// and the underlying domain storage is order-independent.
func (b *bufferedTrieContext) flush(target PatriciaContext) error {
	for key, data := range b.writes {
		if err := target.PutBranch([]byte(key), data, b.prev[key]); err != nil {
			return err
		}
	}
	return nil
}

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
func (p *ConcurrentPatriciaHashed) SetBranchCache(cache *BranchCache) {
	p.root.SetBranchCache(cache)
	// Subtries share the root's persistent cache — reads are LRU-safe,
	// and keys naturally partition by first nibble so contention is minimal.
	for i := range p.mounts {
		p.mounts[i].SetBranchCache(cache)
	}
}
func (p *ConcurrentPatriciaHashed) GetBranchCache() *BranchCache {
	return p.root.branchCache
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

	// Each goroutine buffers PutBranch writes to avoid concurrent writes to
	// the shared MemBatch.  Reads still go through MemBatch (read-only during
	// this phase) with local buffer checked first for consistency.
	var bufMu sync.Mutex
	buffers := make([]*bufferedTrieContext, len(t.nibbles))

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
			bufCtx := newBufferedTrieContext(trieCtx)
			bufMu.Lock()
			buffers[ni] = bufCtx
			bufMu.Unlock()
			phnib.ResetContext(bufCtx)
			cnt := 0
			err := nib.Load(nil, "", func(hashedKey, plainKey []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
				cnt++
				if phnib.trace {
					fmt.Printf("\n%x) %d plainKey [%x] hashedKey [%x] currentKey [%x]\n", ni, cnt, plainKey, hashedKey, phnib.currentKey[:phnib.currentKeyLen])
				}
				if err := phnib.followAndUpdate(hashedKey, plainKey, nil); err != nil {
					return fmt.Errorf("followAndUpdate[%x]: %w", ni, err)
				}
				return nil
			}, etl.TransformArgs{Quit: gctx.Done()})
			if err != nil {
				return err
			}
			if cnt == 0 {
				return nil
			}
			if pph.mounts[ni].trace {
				fmt.Printf("NOW FOLDING nib [%x] #%d d=%d\n", ni, cnt, phnib.depths[0])
			}
			return pph.foldNibble(gctx, ni)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Flush buffered branch writes sequentially through the root context
	for _, buf := range buffers {
		if buf == nil {
			continue
		}
		if err := buf.flush(pph.root.ctx); err != nil {
			return nil, err
		}
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
	// Only evaluate concurrent eligibility when a context factory is
	// available — ParallelHashSort requires per-goroutine DB contexts.
	// Calling SetConcurrentCommitment when CtxFactory is nil would
	// reinitialize ETL collectors via initCollector(), disrupting
	// callers that create SharedDomains without EnableParaTrieDB
	// (e.g. receipts generator, eth_simulateV1).
	if warmup.CtxFactory != nil {
		nextConcurrent, err := p.CanDoConcurrentNext()
		if err != nil {
			return nil, err
		}
		updates.SetConcurrentCommitment(nextConcurrent)
	}
	return rootHash, nil
}

func (p *ConcurrentPatriciaHashed) CanDoConcurrentNext() (bool, error) {
	// ParallelHashSort produces wrong trie roots when run through the real
	// SharedDomains/MemBatch path (confirmed with hive rpc-compat).
	// The algorithm works correctly with mock state (unit tests pass), so
	// the bug is in the interaction between bufferedTrieContext flush and
	// the real DomainPut/GetLatest path — possibly the skip-if-equal
	// optimization in DomainPut or a visibility issue in the history writers.
	//
	// The topology check is correct; re-enable when the DB interaction is fixed.
	// Disabled: see comment above.
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

// ResetMounts resets the 16 mounted subtries without touching the root.
// Used after restoring root state (SetState) to discard stale subtrie data.
func (p *ConcurrentPatriciaHashed) ResetMounts() {
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
