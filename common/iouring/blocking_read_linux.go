// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

//go:build linux

package iouring

import (
	"os"
	"runtime"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
)

// Pool of small rings for blocking async reads. Each goroutine borrows a ring,
// waits for one read, and returns it.

// MaxReadSize covers a maximum-size contract in one read. The
// residency gate must cap its window at this size.
var MaxReadSize = max(64*1024, 8*os.Getpagesize())

// poolSize bounds concurrent reads at ~2*GOMAXPROCS because each io_uring read
// releases a P for another goroutine. The cap also limits pinned kernel memory.
func poolSize() int {
	if n := dbg.EnvInt("RESIDENCY_IOURING_RINGS", 0); n > 0 {
		return n
	}
	return min(2*runtime.GOMAXPROCS(0), 64)
}

type pooledRing struct {
	r    *Ring
	buf  []byte
	offs [1]int64
	lens [1]int
	bufs [1][]byte
}

var (
	// ringPool is nil when io_uring is unavailable. BlockingRead then no-ops and
	// callers fall back to ordinary blocking mmap faults.
	ringPool     chan *pooledRing
	ringPoolOnce sync.Once
)

func initPool() {
	n := poolSize()
	pool := make(chan *pooledRing, n)
	for range n {
		r, err := New(8)
		if err != nil { // io_uring unavailable: leave ringPool nil, close any partial pool
			close(pool)
			for pr := range pool {
				pr.r.Close()
			}
			return
		}
		pool <- &pooledRing{r: r, buf: make([]byte, MaxReadSize)}
	}
	ringPool = pool
}

// BlockingRead reads [off, off+length) into an internal scratch buffer through
// io_uring and waits for completion without holding the goroutine's P. It is a
// no-op if io_uring is unavailable.
func BlockingRead(fd int, off int64, length int) {
	ringPoolOnce.Do(initPool)
	if ringPool == nil {
		return
	}
	if length > MaxReadSize {
		length = MaxReadSize
	}
	pr := <-ringPool
	pr.offs[0], pr.lens[0], pr.bufs[0] = off, length, pr.buf
	if err := pr.r.BlockingBatchRead(fd, pr.offs[:], pr.lens[:], pr.bufs[:]); err != nil {
		pr.r.reset()
	}
	ringPool <- pr
}

// Available reports whether io_uring can be set up in this process (kernel support,
// not blocked by seccomp). Used to warn at startup and to skip io_uring tests.
func Available() bool {
	r, err := New(1)
	if err != nil {
		return false
	}
	r.Close()
	return true
}
