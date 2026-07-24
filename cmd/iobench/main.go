// iobench: pure random-read parallelization microbench. Reads random page-aligned
// offsets from a big file either via blocking mmap touch (fault holds the P) or via
// io_uring (releases the P). No decompression/index/db — just the read primitive, so
// the effect of GOMAXPROCS vs workers vs read-mode is isolated.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/iouring"
)

const pageSize = 4096

func main() {
	file := flag.String("file", "/erigon-data/iobench.dat", "backing file (created if missing)")
	size := flag.Int64("size", 40<<30, "file size in bytes")
	reads := flag.Int("reads", 1_000_000, "number of random reads")
	workers := flag.Int("workers", 64, "concurrent worker goroutines")
	mode := flag.String("mode", "mmap", "mmap (blocking fault) | iouring (async)")
	flag.Parse()

	ensureFile(*file, *size)
	f, err := os.OpenFile(*file, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fd := int(f.Fd())

	rng := rand.New(rand.NewSource(42)) // fixed seed: same offsets for both modes
	npages := *size / pageSize
	offs := make([]int64, *reads)
	for i := range offs {
		offs[i] = rng.Int63n(npages) * pageSize
	}

	var mm []byte
	if *mode == "mmap" {
		mm, err = unix.Mmap(fd, 0, int(*size), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			panic(err)
		}
		_ = unix.Madvise(mm, unix.MADV_RANDOM) // one page per fault, no readahead (match io_uring's 4KB)
		defer unix.Munmap(mm)
	}

	var sink int64
	idx := int64(-1)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&idx, 1)
				if i >= int64(*reads) {
					return
				}
				off := offs[i]
				if mm != nil {
					atomic.AddInt64(&sink, int64(mm[off])) // touch -> major fault (blocks P)
				} else {
					iouring.WarmOne(fd, off, pageSize) // async read (releases P)
				}
			}
		}()
	}
	wg.Wait()
	el := time.Since(start)
	_ = sink
	fmt.Printf("mode=%-7s reads=%d workers=%-4d GOMAXPROCS=%-3d elapsed=%-12s IOPS=%.0f\n",
		*mode, *reads, *workers, runtime.GOMAXPROCS(0), el.String(), float64(*reads)/el.Seconds())
}

func ensureFile(path string, size int64) {
	if fi, err := os.Stat(path); err == nil && fi.Size() == size {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := make([]byte, 16<<20)
	rng := rand.New(rand.NewSource(7))
	for i := range buf {
		buf[i] = byte(rng.Intn(256))
	}
	var written int64
	for written < size {
		n := int64(len(buf))
		if size-written < n {
			n = size - written
		}
		if _, err := f.Write(buf[:n]); err != nil {
			panic(err)
		}
		written += n
	}
	f.Sync()
	fmt.Fprintf(os.Stderr, "created %s (%d bytes)\n", path, size)
}
