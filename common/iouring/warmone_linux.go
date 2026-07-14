//go:build linux

package iouring

import (
	"os"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
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
		if err != nil { // io_uring is required — there is no fallback path.
			log.Crit("[IOURING] io_uring_setup failed; kernel does not support it or it is blocked (seccomp)", "ring", i, "err", err)
			os.Exit(1)
		}
		ringPool <- &pooledRing{r: r, buf: make([]byte, warmBufSize)}
	}
}

// WarmOne reads [off, off+length) via a pooled io_uring ring to populate the
// page cache. It blocks for a free ring when the pool is drained (parking the
// goroutine, freeing its P). There is no fallback: io_uring must be available,
// and the process exits the first time a warm is needed if setup fails.
func WarmOne(fd int, off int64, length int) {
	ringPoolOnce.Do(initPool)
	if length > warmBufSize {
		length = warmBufSize
	}
	pr := <-ringPool
	_ = pr.r.BatchReadWarm(fd, []int64{off}, []int{length}, [][]byte{pr.buf})
	ringPool <- pr
}
