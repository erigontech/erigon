package commitment

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon-lib/etl"
	"golang.org/x/sync/errgroup"
)

// if nibble set is -1 then subtrie is not mounted to the nibble, but limited by depth: eg do not fold mounted trie above depth 63
func (hph *HexPatriciaHashed) mountTo(root *HexPatriciaHashed, nibble int) bool {
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
	//root.mountedTries = append(root.mountedTries, hph) // TODO clean up

	shouldSequential := false

	for row := 0; row <= hph.activeRows; row++ {
		for nib := 0; nib < len(hph.grid[row]); nib++ {
			hph.grid[row][nib] = root.grid[row][nib]
			if nib == nibble && hph.grid[row][nib].IsEmpty() {
				shouldSequential = true
			}
		}
	}
	return shouldSequential
}

type ConcurrentPatriciaHashed struct {
	root   *HexPatriciaHashed
	rootMu sync.Mutex
	mounts [16]*HexPatriciaHashed
}

// Subtrie inherits root state, address length
func NewConcurrentPatriciaHashed(root *HexPatriciaHashed) *ConcurrentPatriciaHashed {
	p := &ConcurrentPatriciaHashed{root: root}
	for i := range p.mounts {
		p.mounts[i] = p.root.SpawnSubTrie(i)
	}
	return p
}

func (p *ConcurrentPatriciaHashed) RootTrie() *HexPatriciaHashed {
	return p.root
}

func (p *ConcurrentPatriciaHashed) foldNibble(nib int) error {
	c, err := p.mounts[nib].foldMounted(nib)
	if err != nil {
		return err
	}

	// protects modification of top level branch kept within root trie
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

func (p *ConcurrentPatriciaHashed) unfoldRoot() (bool, error) {
	if p.root.trace {
		fmt.Printf("=============ROOT unfold============\n")
	}
	// if p.root.rootPresent && p.root.root.hashedExtLen == 0 { // if root has no extension, we have to unfold
	//zero := []byte{0}
	//for unfolding := p.root.needUnfolding(zero); unfolding > 0; unfolding = p.root.needUnfolding(zero) {
	if unfolding := p.root.needUnfolding(emptyPrefix); unfolding > 0 {
		if err := p.root.unfold(emptyPrefix, unfolding); err != nil {
			return false, fmt.Errorf("unfold: %w", err)
		}
	}
	// }

	var shouldSeq bool
	for nib := range p.mounts {
		if p.mounts[nib].mountTo(p.root, nib) {
			shouldSeq = true
		}
	}
	if p.root.trace {
		fmt.Printf("=========ROOT unfolded============\n")
	}
	return shouldSeq, nil
}

func (p *ConcurrentPatriciaHashed) Close() {
	for i := range p.mounts {
		p.mounts[i].Reset()
	}
}

func (p *ConcurrentPatriciaHashed) SetTrace(b bool) {
	p.root.SetTrace(b)
	for i := range p.mounts {
		p.mounts[i].SetTrace(b)
	}
}

// pass -1 to enable trace just for root trie
func (p *ConcurrentPatriciaHashed) SetParticularTrace(b bool, n int) {
	p.root.SetTrace(b)
	if n < len(p.mounts) && n >= 0 {
		p.mounts[n].SetTrace(b)
	}
}

func (t *Updates) ParallelHashSort(ctx context.Context, pph *ConcurrentPatriciaHashed) ([]byte, error) {
	if t.mode != ModeDirect {
		return nil, errors.New("parallel hashsort for indirect mode is not supported")
	}
	if !t.sortPerNibble {
		return nil, errors.New("sortPerNibble disabled")
	}

	shouldSeq, err := pph.unfoldRoot()
	if err != nil {
		return nil, err
	}

	concurrency := 16
	if shouldSeq {
		fmt.Printf("Reset || to sequential mode\n")
		concurrency = 1
	}

	clear(t.keys)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	processed := new(atomic.Uint64)
	started := time.Now()
	logEvery := time.NewTicker(time.Second * 20)
	defer logEvery.Stop()

	for n := 0; n < len(t.nibbles); n++ {
		ni := n

		g.Go(func() error {
			cnt := 0
			n := ni
			nib := t.nibbles[n]
			phnib := pph.mounts[n]
			if shouldSeq {
				phnib = pph.root // if sequential, use root trie
			}

			thisNib := byte(ni)
			started := time.Now()
			err := nib.Load(nil, "", func(hashedKey, plainKey []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
				if hashedKey[0] != thisNib {
					return fmt.Errorf("hashedKey[0] %x != nibble %x", hashedKey[0], nib)
				}
				processed.Add(1)
				cnt++
				if phnib.trace {
					fmt.Printf("\n%x) %d plainKey [%x] hashedKey [%x] currentKey [%x]\n", ni, cnt, plainKey, hashedKey, phnib.currentKey[:phnib.currentKeyLen])
				}
				if len(plainKey) == 0 {
					fmt.Printf("empty key %x\n", hashedKey)
				}
				if err := phnib.followAndUpdate(hashedKey, plainKey, nil); err != nil {
					return fmt.Errorf("followAndUpdate[%x]: %w", ni, err)
				}
				return nil
			}, etl.TransformArgs{Quit: ctx.Done()})
			if err != nil {
				return err
			}
			if pph.mounts[ni].trace {
				//fmt.Printf("NOW FOLDING nib [%x] #%d d=%d\n", ni, cnt, phnib.depths[0])
				defer func() {
					fmt.Printf("FOLDED nib [%x] #%d d=%d %v\n", ni, cnt, phnib.depths[0], time.Since(started))
				}()
			}
			if cnt == 0 || shouldSeq {
				return nil
			}
			return pph.foldNibble(ni)
		})
	}
	go func() {
		for {
			select {
			case <-logEvery.C:
				fmt.Printf("concurrently processed %v keys, time spent %v\n", common.PrettyCounter(processed.Load()), time.Since(started))
			case <-ctx.Done():
				return
			}
		}
	}()
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
func (p *ConcurrentPatriciaHashed) Process(ctx context.Context, updates *Updates, logPrefix string) (rootHash []byte, err error) {
	start := time.Now()
	wasConcurrent := updates.IsConcurrentCommitment()
	updCount := updates.Size()
	defer func(s time.Time, wasConcurrent bool) {
		fmt.Printf("commitment time %s; keys %s; was concurrent: %t\n", time.Since(s), common.PrettyCounter(updCount), wasConcurrent)
	}(start, wasConcurrent)

	switch wasConcurrent {
	case true:
		rootHash, err = updates.ParallelHashSort(ctx, p)
	default:
		rootHash, err = p.root.Process(ctx, updates, logPrefix)
	}
	if err != nil {
		return nil, err
	}

	nextConcurrent, err := p.CanDoConcurrentNext()
	if err != nil {
		return nil, err
	}
	updates.SetConcurrentCommitment(nextConcurrent)
	return rootHash, nil
}

func (p *ConcurrentPatriciaHashed) Warmup(ctx PatriciaContext, hashedKey []byte) error {
	if p.root.trace {
		fmt.Printf("WARMUP %x\n", hashedKey)
	}
	//p.root.ResetContext(ctx)
	//if err := p.root.Warmup(ctx, hashedKey); err != nil {
	//	return fmt.Errorf("warmup: %w", err)
	//}
	if hashedKey[0] > 15 {
		return nil // key supposed to be hashed and nibblised, so if nibble is > 15, it is not a key for this trie
	}
	//p.mounts[hashedKey[0]].ResetContext(ctx)
	if err := p.mounts[hashedKey[0]].Warmup(ctx, hashedKey); err != nil {
		return fmt.Errorf("warmup %x: %w", hashedKey[0], err)
	}
	return nil
}

var emptyPrefix = hexNibblesToCompactBytes([]byte{0})

func (p *ConcurrentPatriciaHashed) CanDoConcurrentNext() (bool, error) {
	if p.root.root.extLen == 0 {
		zeroPrefixBranch, _, err := p.root.ctx.Branch(emptyPrefix)
		if err != nil {
			return false, fmt.Errorf("checking shortes prefix branch failed: %w", err)
		}
		// ok branch exists, lets check if each nibble presented.
		if BranchData(zeroPrefixBranch).IsFull() {
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

// Set context for state IO for main trie
func (p *ConcurrentPatriciaHashed) ResetContext(ctx PatriciaContext) {
	p.root.ResetContext(ctx)
}

// ResetMountContext sets context for subtrie with index i
func (p *ConcurrentPatriciaHashed) ResetMountContext(ctx PatriciaContext, i uint) {
	p.mounts[i].ResetContext(ctx)
}

func (p *ConcurrentPatriciaHashed) RootHash() (hash []byte, err error) {
	return p.root.RootHash()
}
