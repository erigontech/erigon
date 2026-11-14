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

// Package iouring provides blocking file IO through io_uring without holding the goroutine's P.
package iouring

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	opRead                 = 22
	opWrite                = 23
	opFsync                = 3
	sqeIODrain             = 1 << 1
	sqeIOLink              = 1 << 2
	enterGetevents         = 1 // IORING_ENTER_GETEVENTS
	offSQRing      uintptr = 0
	offCQRing      uintptr = 0x8000000
	offSQEs        uintptr = 0x10000000

	sqeSize = 64
	cqeSize = 16
)

var ioUringEnter = func(fd int, toSubmit, minComplete, flags uint32) (uintptr, unix.Errno) {
	taken, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
	return taken, errno
}

// io_uring_params layout (Linux). Only the fields we read are named precisely.
type params struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFD         uint32
	resv         [3]uint32
	sqOff        sqringOffsets
	cqOff        cqringOffsets
}

type sqringOffsets struct {
	head, tail, ringMask, ringEntries, flags, dropped, array, resv1 uint32
	resv2                                                           uint64
}
type cqringOffsets struct {
	head, tail, ringMask, ringEntries, overflow, cqes, flags, resv1 uint32
	resv2                                                           uint64
}

type Ring struct {
	fd      int
	entries uint32

	sqRing []byte
	cqRing []byte
	sqes   []byte

	// pointers into sqRing/cqRing
	sqHead, sqTail *uint32
	sqMask         uint32
	sqArray        []uint32
	cqHead, cqTail *uint32
	cqMask         uint32
	cqes           uint32 // offset of the CQE array in cqRing
}

func p32(b []byte, off uint32) *uint32 { return (*uint32)(unsafe.Pointer(&b[off])) }

// New sets up a ring sized for `entries` in-flight requests (rounded up to pow2 by kernel).
func New(entries uint32) (*Ring, error) {
	var pp params
	r1, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(&pp)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %w", errno)
	}
	ring := &Ring{fd: int(r1), entries: pp.sqEntries}

	sqRingSize := pp.sqOff.array + pp.sqEntries*4
	cqRingSize := pp.cqOff.cqes + pp.cqEntries*cqeSize

	// On any mmap error, Close() unmaps whatever succeeded (it tolerates nil
	// slices) and closes the fd — closing the fd alone does not unmap the rings.
	var err error
	ring.sqRing, err = unix.Mmap(ring.fd, int64(offSQRing), int(sqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		ring.Close()
		return nil, fmt.Errorf("mmap sq: %w", err)
	}
	ring.cqRing, err = unix.Mmap(ring.fd, int64(offCQRing), int(cqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		ring.Close()
		return nil, fmt.Errorf("mmap cq: %w", err)
	}
	ring.sqes, err = unix.Mmap(ring.fd, int64(offSQEs), int(pp.sqEntries*sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		ring.Close()
		return nil, fmt.Errorf("mmap sqes: %w", err)
	}

	ring.sqHead = p32(ring.sqRing, pp.sqOff.head)
	ring.sqTail = p32(ring.sqRing, pp.sqOff.tail)
	ring.sqMask = *p32(ring.sqRing, pp.sqOff.ringMask)
	arrayPtr := unsafe.Pointer(&ring.sqRing[pp.sqOff.array])
	ring.sqArray = unsafe.Slice((*uint32)(arrayPtr), pp.sqEntries)

	ring.cqHead = p32(ring.cqRing, pp.cqOff.head)
	ring.cqTail = p32(ring.cqRing, pp.cqOff.tail)
	ring.cqMask = *p32(ring.cqRing, pp.cqOff.ringMask)
	ring.cqes = pp.cqOff.cqes

	return ring, nil
}

// ErrRingReset reports a batch that failed with no read left in flight: the ring was reset and stays usable.
var ErrRingReset = errors.New("io_uring: ring reset with nothing in flight")

// reset resyncs the cached indices to the kernel's, dropping unsubmitted SQEs and unreaped CQEs.
func (r *Ring) reset() {
	atomic.StoreUint32(r.sqTail, atomic.LoadUint32(r.sqHead))
	atomic.StoreUint32(r.cqHead, atomic.LoadUint32(r.cqTail))
}

// abandoned keeps failed rings and their read buffers alive: the kernel may still write into them.
var (
	abandonedMu sync.Mutex
	abandoned   []abandonedRing
)

type abandonedRing struct {
	r    *Ring
	bufs [][]byte
}

// Abandon retires a ring whose reads may still be in flight: the ring and bufs stay alive and are never reused.
func (r *Ring) Abandon(bufs ...[]byte) {
	abandonedMu.Lock()
	defer abandonedMu.Unlock()
	abandoned = append(abandoned, abandonedRing{r, bufs})
}

func (r *Ring) Close() {
	if r.sqRing != nil {
		_ = unix.Munmap(r.sqRing)
	}
	if r.cqRing != nil {
		_ = unix.Munmap(r.cqRing)
	}
	if r.sqes != nil {
		_ = unix.Munmap(r.sqes)
	}
	_ = unix.Close(r.fd)
}

func (r *Ring) fillSQE(idx uint32, fd int, off uint64, buf []byte, userData uint64) {
	base := unsafe.Pointer(&r.sqes[idx*sqeSize])
	s := unsafe.Slice((*byte)(base), sqeSize)
	for i := range s {
		s[i] = 0
	}
	*(*uint8)(base) = opRead                                                    // opcode @0
	*(*int32)(unsafe.Add(base, 4)) = int32(fd)                                  // fd @4
	*(*uint64)(unsafe.Add(base, 8)) = off                                       // off @8
	*(*uint64)(unsafe.Add(base, 16)) = uint64(uintptr(unsafe.Pointer(&buf[0]))) // addr @16
	*(*uint32)(unsafe.Add(base, 24)) = uint32(len(buf))                         // len @24
	*(*uint64)(unsafe.Add(base, 32)) = userData                                 // user_data @32
}

func (r *Ring) complete(count int, process func(uint64, int32) error) error {
	submitted, reaped := 0, 0
	for reaped < count {
		head := atomic.LoadUint32(r.cqHead)
		tail := atomic.LoadUint32(r.cqTail)
		for head != tail && reaped < count {
			cqe := r.cqes + (head&r.cqMask)*cqeSize
			id := *(*uint64)(unsafe.Pointer(&r.cqRing[cqe]))
			res := int32(*p32(r.cqRing, cqe+8))
			if err := process(id, res); err != nil {
				return err
			}
			head++
			reaped++
		}
		atomic.StoreUint32(r.cqHead, head)
		if reaped == count {
			break
		}
		taken, errno := ioUringEnter(r.fd, uint32(count-submitted), uint32(count-reaped), enterGetevents)
		if errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			if submitted == reaped {
				r.reset()
				return errors.Join(ErrRingReset, errno)
			}
			return errno
		}
		submitted += int(taken)
	}
	return nil
}

// ErrReadFailed marks a read that completed with an error; the ring itself still works.
var ErrReadFailed = errors.New("io_uring read failed")

// BlockingBatchRead reads each (offset,len) request into scratch buffers using
// io_uring, keeping up to the ring size in flight. It returns after all complete. After
// ErrReadFailed or ErrRingReset no read is in flight, so the ring and scratch can be reused; after any
// other error reads may still be in flight into scratch: the caller must not reuse them.
func (r *Ring) BlockingBatchRead(fd int, offsets []int64, lens []int, scratch [][]byte) error {
	n := len(offsets)
	done := 0
	for done < n {
		// Submit a wave up to ring capacity.
		tail := atomic.LoadUint32(r.sqTail)
		head := atomic.LoadUint32(r.sqHead)
		space := r.entries - (tail - head)
		wave := 0
		for wave < int(space) && done+wave < n {
			i := done + wave
			slot := tail & r.sqMask
			r.fillSQE(slot, fd, uint64(offsets[i]), scratch[wave][:lens[i]], uint64(i))
			r.sqArray[slot] = slot
			tail++
			wave++
		}
		// Release-store the tail so the SQE bodies and sqArray writes above are
		// visible to the kernel before it observes the advanced tail.
		atomic.StoreUint32(r.sqTail, tail)

		var failed error
		if err := r.complete(wave, func(id uint64, res int32) error {
			if res < 0 && failed == nil {
				failed = fmt.Errorf("%w: request %d: %w", ErrReadFailed, id, unix.Errno(-res))
			}
			return nil
		}); err != nil {
			return fmt.Errorf("io_uring_enter: %w", err)
		}
		if failed != nil {
			return failed
		}
		done += wave
	}
	return nil
}
