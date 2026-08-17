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

// Package iouring is a minimal hand-rolled io_uring facility for file reads at a
// controlled queue depth — enough to warm the page cache without blocking the
// goroutine's P (io_uring_enter goes through the syscall path, so a slow read
// hands the P off). Not a general async reactor: BatchReadWarm submits a batch
// and waits for all of it.
package iouring

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	opRead                 = 22 // IORING_OP_READ
	enterGetevents         = 1  // IORING_ENTER_GETEVENTS
	offSQRing      uintptr = 0
	offCQRing      uintptr = 0x8000000
	offSQEs        uintptr = 0x10000000

	sqeSize = 64
	cqeSize = 16
)

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

	return ring, nil
}

// reset resyncs the cached indices to the kernel's, discarding any half-submitted
// SQE or pending CQE left by a failed io_uring_enter so the ring is reusable. Safe
// for warming: a dropped completion just means a page isn't pre-warmed and takes an
// ordinary fault.
func (r *Ring) reset() {
	atomic.StoreUint32(r.sqTail, atomic.LoadUint32(r.sqHead))
	atomic.StoreUint32(r.cqHead, atomic.LoadUint32(r.cqTail))
}

func (r *Ring) Close() {
	if r.sqRing != nil {
		unix.Munmap(r.sqRing)
	}
	if r.cqRing != nil {
		unix.Munmap(r.cqRing)
	}
	if r.sqes != nil {
		unix.Munmap(r.sqes)
	}
	unix.Close(r.fd)
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

// BatchReadWarm reads each (offset,len) request into scratch buffers using
// io_uring, keeping up to the ring size in flight. Returns after all complete.
// The reads populate the page cache; buffer contents are discarded.
func (r *Ring) BatchReadWarm(fd int, offsets []int64, lens []int, scratch [][]byte) error {
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

		_, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(r.fd), uintptr(wave), uintptr(wave), enterGetevents, 0, 0)
		if errno != 0 {
			return fmt.Errorf("io_uring_enter: %w", errno)
		}

		// Reap `wave` completions.
		reaped := 0
		for reaped < wave {
			ch := atomic.LoadUint32(r.cqHead)
			ct := atomic.LoadUint32(r.cqTail)
			// A CQE result (res @8) is discarded: a failed warm just means the
			// following mmap access takes an ordinary fault. Only the count matters.
			for ch != ct {
				ch++
				reaped++
			}
			atomic.StoreUint32(r.cqHead, ch)
			if reaped < wave {
				_, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(r.fd), 0, uintptr(wave-reaped), enterGetevents, 0, 0)
				if errno != 0 {
					return fmt.Errorf("io_uring_enter(wait): %w", errno)
				}
			}
		}
		done += wave
	}
	return nil
}
