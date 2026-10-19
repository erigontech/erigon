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

package dir

import "os"

// PacedWriter writes f in chunks: wait runs before each chunk, and WriteBack keeps at most two chunks dirty, so a
// blocked wait also stops the disk writes.
type PacedWriter struct {
	f          *os.File
	chunk, off int64
	wait       func() error
}

func NewPacedWriter(f *os.File, chunk int64, wait func() error) *PacedWriter {
	return &PacedWriter{f: f, chunk: chunk, wait: wait}
}

func (w *PacedWriter) Write(p []byte) (written int, err error) {
	for len(p) > 0 {
		if w.off%w.chunk == 0 {
			if err = w.wait(); err != nil {
				return written, err
			}
		}
		n := min(int64(len(p)), w.chunk-w.off%w.chunk)
		m, werr := w.f.Write(p[:n])
		written += m
		w.off += int64(m)
		if werr != nil {
			return written, werr
		}
		if w.off%w.chunk == 0 {
			if err = WriteBack(w.f, w.off-w.chunk, w.chunk, w.chunk); err != nil {
				return written, err
			}
		}
		p = p[n:]
	}
	return written, nil
}
