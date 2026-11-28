package solid

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/prysmaticlabs/gohashtree"
)

const parallelMinChunks = 64

type validatorRootWorkerPool struct {
	maxWorkers       int
	workerLimit      func() int
	totalHelperLimit func() int
	minChunks        int
	liveHelpers      atomic.Int64
	helperHook       func(int)
}

func newValidatorRootWorkerPool(maxWorkers int) *validatorRootWorkerPool {
	maxWorkers = max(1, maxWorkers)
	return &validatorRootWorkerPool{
		maxWorkers:       maxWorkers,
		workerLimit:      func() int { return maxWorkers },
		totalHelperLimit: func() int { return maxWorkers - 1 },
		minChunks:        parallelMinChunks,
	}
}

// Each build uses at most half of GOMAXPROCS, while overlapping builds share
// up to GOMAXPROCS helpers instead of one build's share.
func newDefaultValidatorRootWorkerPool() *validatorRootWorkerPool {
	workers := newValidatorRootWorkerPool(8)
	workers.workerLimit = func() int { return max(1, runtime.GOMAXPROCS(0)/2) }
	workers.totalHelperLimit = func() int { return runtime.GOMAXPROCS(0) }
	return workers
}

var validatorRootWorkers = newDefaultValidatorRootWorkerPool()

func (p *validatorRootWorkerPool) workerCount() int {
	return min(p.maxWorkers, max(1, p.workerLimit()))
}

func (p *validatorRootWorkerPool) acquireHelpers(chunkCount int) int {
	if chunkCount < p.minChunks {
		return 0
	}
	helperCount := 0
	buildHelperLimit := p.workerCount() - 1
	totalHelperLimit := int64(p.totalHelperLimit())
	for helperCount < buildHelperLimit {
		liveHelpers := p.liveHelpers.Load()
		if liveHelpers >= totalHelperLimit {
			return helperCount
		}
		if p.liveHelpers.CompareAndSwap(liveHelpers, liveHelpers+1) {
			helperCount++
			if p.helperHook != nil {
				p.helperHook(1)
			}
		}
	}
	return helperCount
}

func (p *validatorRootWorkerPool) releaseHelpers(helperCount int) {
	for range helperCount {
		p.liveHelpers.Add(-1)
		if p.helperHook != nil {
			p.helperHook(-1)
		}
	}
}

var validatorRootScratch = sync.Pool{New: func() any { return new([8][32]byte) }}

func validatorLeaf(elem, out []byte) {
	chunks := validatorRootScratch.Get().(*[8][32]byte)
	validatorRoot(Validator(elem), out, chunks)
	validatorRootScratch.Put(chunks)
}

func validatorRoot(validator Validator, out []byte, chunks *[8][32]byte) {
	clear(chunks[:])
	copy(chunks[0][:], validator[:32])
	copy(chunks[1][:16], validator[32:48])
	if err := gohashtree.Hash(chunks[:1], chunks[:2]); err != nil {
		panic(err)
	}
	copy(chunks[1][:], validator[48:80])
	copy(chunks[2][:8], validator[80:88])
	chunks[3][0] = validator[88]
	copy(chunks[4][:8], validator[89:97])
	copy(chunks[5][:8], validator[97:105])
	copy(chunks[6][:8], validator[105:113])
	copy(chunks[7][:8], validator[113:121])
	if err := gohashtree.Hash(chunks[:4], chunks[:8]); err != nil {
		panic(err)
	}
	if err := gohashtree.Hash(chunks[:2], chunks[:4]); err != nil {
		panic(err)
	}
	if err := gohashtree.Hash(chunks[:1], chunks[:2]); err != nil {
		panic(err)
	}
	copy(out, chunks[0][:])
}
