//go:build linux

package iouring

import (
	"fmt"
	"os"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
)

// Pool of small rings for per-access warming: each goroutine borrows a ring,
// does a single-read submit+wait (which releases the P via io_uring_enter),
// and returns it. Sized to the expected number of concurrent cold readers.
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
}

// WarmOne reads [off, off+length) via a pooled io_uring ring to populate the
// page cache. Returns false if io_uring is unavailable or the pool is exhausted,
// in which case the caller should fall back to a blocking read.
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
