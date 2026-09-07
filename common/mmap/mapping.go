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

package mmap

import (
	"fmt"
	"os"

	mmapgo "github.com/edsrzf/mmap-go"
)

// Ro is a read-only file mapping. Writing through it faults.
type Ro []byte

// Rw is a read-write file mapping. Changes reach the file lazily; call Sync to
// force them out.
type Rw []byte

// OpenRo by-default sets MADV_RANDOM - because all files size >> RAM
// mmap 1 file multiple times with different `madv` - is ok (Linux supports)
// mmap 1 file-region multiple times with different `madv` - is ok (Linux supports)
func OpenRo(f *os.File, size int) (Ro, error) {
	if err := checkSize(f, size); err != nil {
		return nil, err
	}
	m, err := mmapgo.MapRegion(f, size, mmapgo.RDONLY, 0, 0)
	if err != nil {
		return nil, err
	}
	if err := MadviseRandom(m); err != nil { // ENOSYS is already tolerated inside
		_ = m.Unmap()
		return nil, err
	}
	return Ro(m), nil
}

func OpenRw(f *os.File, size int) (Rw, error) {
	if err := checkSize(f, size); err != nil {
		return nil, err
	}
	m, err := mmapgo.MapRegion(f, size, mmapgo.RDWR, 0, 0)
	if err != nil {
		return nil, err
	}
	return Rw(m), nil
}

// checkSize keeps both platforms alike: unix rejects size<=0 with EINVAL, while
// Windows maps the whole file and returns a zero-length slice that Unmap skips,
// leaking the view.
func checkSize(f *os.File, size int) error {
	if size <= 0 {
		return fmt.Errorf("mmap %s: size must be positive, got %d", f.Name(), size)
	}
	return nil
}

// Unmap releases the mapping and nils m, so a use-after-unmap panics on a nil slice instead of
// reading whatever the kernel put at that address next.
func (m *Ro) Unmap() error {
	if len(*m) == 0 {
		*m = nil
		return nil
	}
	return (*mmapgo.MMap)(m).Unmap()
}

func (m *Rw) Unmap() error {
	if len(*m) == 0 {
		*m = nil
		return nil
	}
	return (*mmapgo.MMap)(m).Unmap()
}

func (m Rw) Sync() error { return mmapgo.MMap(m).Flush() }
