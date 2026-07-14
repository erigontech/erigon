//go:build linux

package iouring

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/dbg"
)

// Pool of small rings for per-access warming: each goroutine borrows a ring,
// does a single-read submit+wait (which releases the P via io_uring_enter),
// and returns it. Sized to expected concurrent cold readers.
const warmBufSize = 32 * 1024

type pooledRing struct {
	r   *Ring
	buf []byte
}

var (
	ringPool     chan *pooledRing
	ringPoolOnce sync.Once
)

func initPool() {
	n := dbg.EnvInt("RESIDENCY_IOURING_RINGS", 128)
	ringPool = make(chan *pooledRing, n)
	for i := 0; i < n; i++ {
		r, err := New(8)
		if err != nil { // io_uring unavailable (e.g. seccomp): leave pool empty → WarmOne returns false
			fmt.Fprintf(os.Stderr, "[IOURING] setup FAILED after %d rings: %v — falling back to pread\n", i, err)
			return
		}
		ringPool <- &pooledRing{r: r, buf: make([]byte, warmBufSize)}
	}
	fmt.Fprintf(os.Stderr, "[IOURING] initialized %d rings OK\n", n)
}

// Batch prefetch: pool of deep (QD) rings for warming many reads across files
// in one wave.
const batchQD = 128

type pooledBatch struct {
	r       *Ring
	scratch [][]byte
}

var (
	batchPool chan *pooledBatch
	batchOnce sync.Once
	batchOK   atomic.Bool
)

func initBatchPool() {
	n := dbg.EnvInt("IOURING_BATCH_RINGS", 4)
	batchPool = make(chan *pooledBatch, n)
	for i := 0; i < n; i++ {
		r, err := New(batchQD)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[IOURING] batch pool setup FAILED after %d: %v\n", i, err)
			return
		}
		sc := make([][]byte, r.entries)
		for j := range sc {
			sc[j] = make([]byte, warmBufSize)
		}
		batchPool <- &pooledBatch{r: r, scratch: sc}
	}
	batchOK.Store(true)
	fmt.Fprintf(os.Stderr, "[IOURING] batch pool %d rings x QD%d OK\n", n, batchQD)
}

// WarmMany submits all reqs as a deep io_uring wave to warm the page cache.
// Returns false if io_uring is unavailable.
func WarmMany(reqs []Req) bool {
	batchOnce.Do(initBatchPool)
	if !batchOK.Load() || len(reqs) == 0 {
		return false
	}
	pb := <-batchPool
	err := pb.r.BatchReadMulti(reqs, pb.scratch)
	batchPool <- pb
	return err == nil
}

// WarmOne reads [off, off+length) via a pooled io_uring ring to populate the
// page cache. Returns false if io_uring is unavailable (caller should fall back).
func WarmOne(fd int, off int64, length int) bool {
	ringPoolOnce.Do(initPool)
	if length > warmBufSize {
		length = warmBufSize
	}
	select {
	case pr := <-ringPool:
		err := pr.r.BatchReadWarm(fd, []int64{off}, []int{length}, [][]byte{pr.buf})
		ringPool <- pr
		return err == nil
	default:
		return false // pool exhausted → caller falls back to pread
	}
}
