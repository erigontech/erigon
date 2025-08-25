// Copyright 2024 The Erigon Authors
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

package seg

import (
	"sync"
	"unsafe"

	"github.com/edsrzf/mmap-go"

	"github.com/erigontech/erigon-lib/log/v3"
)

// DecompressArena - non-thread-safe off-heap arena-allocator - designed as a Decompression target. It allows high-level-API to be zero-copy-like:
//   - `v := tx.Get(k)` can have "v - valid until end of `tx`" semantic
//   - If underlying file is uncompressed - it will return pointer to MMAP
//   - If underlying file is compressed - it will return pointer to `DecompressArena` bounded to `tx` object
//
// Uses MAP_ANON for anonymous memory mapping, valid for transaction lifetime
type DecompressArena struct {
	mem    []mmap.MMap
	memI   int
	cap    int
	offset int
}

const (
	defaultCacheSize = 64 * 1024 * 1024 // 64MB default
)

// NewDecompressArena creates a new mmap-based decompression cache
func NewDecompressArena(cap int) (*DecompressArena, error) {
	if cap <= 0 {
		cap = defaultCacheSize
	}
	d := &DecompressArena{
		cap:  cap,
		memI: -1,
	}
	return d, nil
}

func (c *DecompressArena) newChunk() error {
	mmapData, err := mmap.MapRegion(nil, c.cap, mmap.RDWR, mmap.ANON, 0)
	if err != nil {
		return err
	}
	c.mem = append(c.mem, mmapData)
	c.memI++
	c.offset = 0
	return nil
}

func (c *DecompressArena) Close() {
	c.Free()
}

// Free idempotent, all allocated valued become invalid. Batch free arena
func (c *DecompressArena) Free() {
	if c.mem != nil {
		for _, m := range c.mem {
			if err := m.Unmap(); err != nil {
				log.Warn("DecompressArena: failed to unmap memory", "err", err)
			}
		}
		c.mem = nil
	}
}

func (c *DecompressArena) NoSpaceLeft() bool { return c.cap <= c.offset }

const Alignment = 8
const AllocLimitOnArena = 64 * 1024

func (c *DecompressArena) Allocate(size int) []byte {
	if size >= AllocLimitOnArena {
		return make([]byte, size)
	}

	low := c.offset
	c.offset += size
	newOffset := (c.offset + Alignment - 1) / Alignment * Alignment
	if newOffset >= c.cap || c.mem == nil { //fallback to normal allocation - it doesn't reduce value-lifetime guaranties (valid until end of Txn)
		if err := c.newChunk(); err != nil {
			panic(err)
		}
	}
	return c.mem[c.memI][low : low+size : low+size] // https://go.dev/ref/spec#Slicel_expressions
}

type DecompressArenaSlice struct {
	mem    []byte
	cap    int
	offset int
}

const poolBufCap = 128 * 1024

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, poolBufCap)
	},
}

func NewDecompressArenaSlice(cap int) (*DecompressArenaSlice, error) {
	return &DecompressArenaSlice{
		cap: poolBufCap,
	}, nil
}

func (c *DecompressArenaSlice) Close() {
	c.Free()
}

// Free idempotent, all allocated valued become invalid. Batch free arena
func (c *DecompressArenaSlice) Free() {
	if c.mem != nil {
		bufPool.Put(c.mem)
		c.mem = nil
	}
}

func (c *DecompressArenaSlice) NoSpaceLeft() bool { return c.cap <= c.offset }

func (c *DecompressArenaSlice) Allocate(size int) []byte {
	if size >= AllocLimitOnArena {
		return make([]byte, size)
	}

	low := c.offset
	c.offset += size
	newOffset := (c.offset + Alignment - 1) / Alignment * Alignment
	if newOffset >= c.cap || c.mem == nil { //fallback to normal allocation - it doesn't reduce value-lifetime guaranties (valid until end of Txn)
		if c.mem != nil {
			bufPool.Put(c.mem)
		}
		c.mem = bufPool.Get().([]byte)
		c.offset = 0
	}
	return unsafe.Slice(&c.mem[low], size)
	//return c.mem[low : low+size : low+size] // https://go.dev/ref/spec#Slicel_expressions
}

type DecompressArenaSlice2 struct {
	mem    []byte
	cap    int
	offset int
}

func NewDecompressArenaSlice2(cap int) (*DecompressArenaSlice2, error) {
	return &DecompressArenaSlice2{
		cap: cap,
	}, nil
}

func (c *DecompressArenaSlice2) Close() {
	c.Free()
}

// Free idempotent, all allocated valued become invalid. Batch free arena
func (c *DecompressArenaSlice2) Free() {
	if c.mem != nil {
		c.mem = nil
	}
}

func (c *DecompressArenaSlice2) Allocate(size int) []byte {
	if size >= AllocLimitOnArena {
		return make([]byte, size)
	}

	low := c.offset
	c.offset += size
	newOffset := (c.offset + Alignment - 1) / Alignment * Alignment
	if newOffset >= c.cap || c.mem == nil { //fallback to normal allocation - it doesn't reduce value-lifetime guaranties (valid until end of Txn)
		c.mem = make([]byte, c.cap)
		c.offset = 0
	}
	return unsafe.Slice(&c.mem[low], size)
	//return c.mem[low : low+size : low+size] // https://go.dev/ref/spec#Slicel_expressions
}
