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

package iouring

import (
	"errors"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sys/unix"
)

type Writer struct {
	file   *os.File
	ring   *Ring
	buf    []byte
	offset int64
	err    error
	closed bool
}

func NewWriter(f *os.File) *Writer {
	offset, err := f.Seek(0, io.SeekCurrent)
	r, _ := New(2)
	return &Writer{file: f, ring: r, buf: make([]byte, 0, int(datasize.MB)), offset: offset, err: err}
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, os.ErrClosed
	}
	if w.err != nil {
		return 0, w.err
	}
	n := 0
	for len(p) > 0 {
		if len(w.buf) == cap(w.buf) {
			if w.err = w.flush(false); w.err != nil {
				return n, w.err
			}
		}
		k := min(cap(w.buf)-len(w.buf), len(p))
		w.buf = append(w.buf, p[:k]...)
		p = p[k:]
		n += k
	}
	return n, nil
}

func (w *Writer) Close() error {
	if w.closed {
		return w.err
	}
	w.closed = true
	if w.err == nil {
		w.err = w.flush(true)
	}
	if w.ring != nil {
		w.ring.Close()
		w.ring = nil
	}
	w.err = errors.Join(w.err, w.file.Close())
	return w.err
}

func (w *Writer) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	if w.ring != nil {
		w.ring.Close()
		w.ring = nil
	}
	_ = w.file.Close()
}

func (w *Writer) flush(final bool) error {
	for {
		if w.ring == nil {
			if len(w.buf) > 0 {
				n, err := w.file.WriteAt(w.buf, w.offset)
				w.offset += int64(n)
				if err != nil {
					return err
				}
				w.buf = w.buf[:0]
			}
			if final {
				return w.file.Sync()
			}
			return nil
		}
		r := w.ring
		tail := atomic.LoadUint32(r.sqTail)
		count := 0
		if len(w.buf) > 0 {
			slot := tail & r.sqMask
			r.fillSQE(slot, int(w.file.Fd()), uint64(w.offset), w.buf, 0)
			s := r.sqes[slot*sqeSize:]
			s[0] = opWrite
			if final {
				s[1] = sqeIOLink
			}
			r.sqArray[slot] = slot
			tail++
			count++
		}
		if final {
			slot := tail & r.sqMask
			s := r.sqes[slot*sqeSize : (slot+1)*sqeSize]
			clear(s)
			s[0] = opFsync
			s[1] = sqeIODrain
			*(*int32)(unsafe.Pointer(&s[4])) = int32(w.file.Fd())
			*(*uint64)(unsafe.Pointer(&s[32])) = uint64(count)
			r.sqArray[slot] = slot
			tail++
			count++
		}
		if count == 0 {
			return nil
		}
		atomic.StoreUint32(r.sqTail, tail)
		var res [2]int32
		err := r.complete(count, func(id uint64, result int32) error {
			if id >= uint64(count) {
				return unix.EIO
			}
			res[id] = result
			return nil
		})
		runtime.KeepAlive(w.buf)
		if err != nil {
			if errors.Is(err, ErrRingReset) {
				r.Close()
				w.ring = nil
				continue
			}
			r.Abandon(w.buf)
			w.ring = nil
			return &os.PathError{Op: "write", Path: w.file.Name(), Err: err}
		}
		if errno := unix.Errno(-res[count-1]); final && res[count-1] < 0 && errno != unix.ECANCELED && errno != unix.EINVAL && errno != unix.EOPNOTSUPP {
			return &os.PathError{Op: "sync", Path: w.file.Name(), Err: errno}
		}
		if len(w.buf) > 0 {
			if res[0] < 0 {
				errno := unix.Errno(-res[0])
				if errno == unix.EINVAL || errno == unix.EOPNOTSUPP {
					r.Close()
					w.ring = nil
					continue
				}
				return &os.PathError{Op: "write", Path: w.file.Name(), Err: errno}
			}
			if res[0] == 0 || int(res[0]) > len(w.buf) {
				return io.ErrShortWrite
			}
			w.offset += int64(res[0])
			if int(res[0]) < len(w.buf) {
				w.buf = w.buf[:copy(w.buf, w.buf[res[0]:])]
				continue
			}
			w.buf = w.buf[:0]
		}
		if final && res[count-1] < 0 {
			errno := unix.Errno(-res[count-1])
			if errno == unix.EINVAL || errno == unix.EOPNOTSUPP {
				r.Close()
				w.ring = nil
				return w.file.Sync()
			}
			return &os.PathError{Op: "sync", Path: w.file.Name(), Err: errno}
		}
		return nil
	}
}
