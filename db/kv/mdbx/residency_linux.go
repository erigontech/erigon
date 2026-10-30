// Copyright 2026 The Erigon Authors
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

package mdbx

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/iouring"
)

// ResidencyGate routes multi-page value reads through blocking async I/O instead
// of letting the mapping fault them in one page at a time.
var ResidencyGate = dbg.EnvBool("MDBX_RESIDENCY", false)

var osPageSize = os.Getpagesize()

// dataMapping is the mdbx.dat region of this process' address space. MDBX maps
// the file whole, so a value pointer's file offset is its distance from the
// mapping start. Cached because /proc/self/maps is expensive to parse, and
// re-resolved when a pointer falls outside — the mapping moves when the map
// size grows.
type dataMapping struct {
	start, end uintptr
	fileOff    int64
	fd         uintptr
}

func (db *MdbxKV) mapping() *dataMapping { return db.dataMap.Load() }

// ValueFileRegion returns the mdbx.dat descriptor and file offset backing v.
// ok is false for a value outside the data mapping — a dirty page in a write
// txn, or a stale cache after a remap.
func (db *MdbxKV) ValueFileRegion(v []byte) (fd uintptr, off int64, ok bool) {
	if len(v) == 0 {
		return 0, 0, false
	}
	p := uintptr(unsafe.Pointer(&v[0]))
	m := db.mapping()
	if m == nil || p < m.start || p+uintptr(len(v)) > m.end {
		if m = db.resolveMapping(p); m == nil {
			return 0, 0, false
		}
		if p < m.start || p+uintptr(len(v)) > m.end {
			return 0, 0, false
		}
	}
	return m.fd, m.fileOff + int64(p-m.start), true
}

// WarmValue reads v's pages through io_uring so the caller's first touch does
// not take a chain of major faults. Single-page values are skipped: the b-tree
// walk that produced v already faulted the leaf holding them.
func (db *MdbxKV) WarmValue(v []byte) {
	if !ResidencyGate || len(v) <= osPageSize {
		return
	}
	fd, off, ok := db.ValueFileRegion(v)
	if !ok {
		return
	}
	aligned := off &^ int64(osPageSize-1)
	n := int(off-aligned) + len(v)
	for n > 0 {
		chunk := min(n, iouring.MaxReadSize)
		iouring.BlockingRead(int(fd), aligned, chunk)
		aligned += int64(chunk)
		n -= chunk
	}
}

func (db *MdbxKV) resolveMapping(p uintptr) *dataMapping {
	fd, err := db.env.FD()
	if err != nil {
		return nil
	}
	dat, err := filepath.Abs(filepath.Join(db.path, "mdbx.dat"))
	if err != nil {
		return nil
	}
	f, err := os.Open("/proc/self/maps")
	if err != nil {
		return nil
	}
	defer f.Close()

	start, end, fileOff, ok := parseMapsRegion(f, dat, p)
	if !ok {
		return nil
	}
	m := &dataMapping{start: start, end: end, fileOff: fileOff, fd: fd}
	db.dataMap.Store(m)
	return m
}

// parseMapsRegion finds the /proc/self/maps line for path that contains addr.
// Lines look like: 7f0c00000000-7f0c40000000 r--s 00000000 fe:01 123 /db/mdbx.dat
func parseMapsRegion(r io.Reader, path string, addr uintptr) (start, end uintptr, fileOff int64, ok bool) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Fields(line)
		if len(fields) < 6 || fields[5] != path {
			continue
		}
		dash := strings.IndexByte(fields[0], '-')
		if dash < 0 {
			continue
		}
		s, err := strconv.ParseUint(fields[0][:dash], 16, 64)
		if err != nil {
			continue
		}
		e, err := strconv.ParseUint(fields[0][dash+1:], 16, 64)
		if err != nil {
			continue
		}
		o, err := strconv.ParseUint(fields[2], 16, 64)
		if err != nil {
			continue
		}
		if addr >= uintptr(s) && addr < uintptr(e) {
			return uintptr(s), uintptr(e), int64(o), true
		}
	}
	return 0, 0, 0, false
}
