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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestWriterWritesAndCloses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output")
	f, err := os.Create(path)
	require.NoError(t, err)
	w := NewWriter(f)
	data := make([]byte, 3<<20+37)
	for i := range data {
		data[i] = byte(i*17 + i/4096)
	}
	for start := 0; start < len(data); {
		end := min(start+32771, len(data))
		n, writeErr := w.Write(data[start:end])
		require.NoError(t, writeErr)
		require.Equal(t, end-start, n)
		start = end
	}
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())
	_, err = w.Write([]byte("closed"))
	require.ErrorIs(t, err, os.ErrClosed)
	actual, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, actual)
}

func TestWriterEmptyAndFailedFile(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprint(closed), func(t *testing.T) {
			f, err := os.Create(filepath.Join(t.TempDir(), "output"))
			require.NoError(t, err)
			w := NewWriter(f)
			if closed {
				require.NoError(t, f.Close())
				_, err = w.Write([]byte("data"))
				require.NoError(t, err)
				require.Error(t, w.Close())
			} else {
				require.NoError(t, w.Close())
			}
		})
	}
}

func TestWriterFallbackCompletions(t *testing.T) {
	for _, errno := range []unix.Errno{unix.EINVAL, unix.EOPNOTSUPP} {
		t.Run(errno.Error(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "output")
			f, err := os.Create(path)
			require.NoError(t, err)
			_, err = f.WriteString("prefix")
			require.NoError(t, err)
			w := NewWriter(f)
			require.NotNil(t, w.ring)
			data := bytes.Repeat([]byte("payload"), 200000)
			n, err := w.Write(data[:100])
			require.NoError(t, err)
			require.Equal(t, 100, n)
			injectCompletion(w.ring, 1, -int32(unix.ECANCELED))
			injectCompletion(w.ring, 0, -int32(errno))
			require.NoError(t, w.flush(true))
			require.Nil(t, w.ring)
			n, err = w.Write(data[100:])
			require.NoError(t, err)
			require.Equal(t, len(data)-100, n)
			require.NoError(t, w.Close())
			actual, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, append([]byte("prefix"), data...), actual)
		})
	}
}

func TestWriterSyncCompletionError(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "output"))
	require.NoError(t, err)
	w := NewWriter(f)
	require.NotNil(t, w.ring)
	injectCompletion(w.ring, 0, -int32(unix.EIO))
	require.ErrorIs(t, w.Close(), unix.EIO)
	require.ErrorIs(t, w.Close(), unix.EIO)
	_, err = f.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestWriterAbortSkipsBufferedData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output")
	f, err := os.Create(path)
	require.NoError(t, err)
	w := NewWriter(f)
	_, err = w.Write([]byte("buffered"))
	require.NoError(t, err)
	w.Abort()
	require.Nil(t, w.ring)
	_, err = f.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Zero(t, info.Size())
	w.Abort()
	require.NoError(t, w.Close())
}

func TestWriterResubmitsShortWrite(t *testing.T) {
	data := make([]byte, 1<<20+100)
	for i := range data {
		data[i] = byte(i%251 + 1)
	}
	open := func(t *testing.T) (*Writer, string) {
		path := filepath.Join(t.TempDir(), "output")
		f, err := os.Create(path)
		require.NoError(t, err)
		_, err = f.WriteString("prefix")
		require.NoError(t, err)
		w := NewWriter(f)
		require.NotNil(t, w.ring)
		return w, path
	}
	requireOutput := func(t *testing.T, path string, hole int, tail []byte) {
		actual, err := os.ReadFile(path)
		require.NoError(t, err)
		expected := append(append([]byte("prefix"), make([]byte, hole)...), tail...)
		require.True(t, bytes.Equal(expected, actual), "got %d bytes, want %d", len(actual), len(expected))
	}
	t.Run("final", func(t *testing.T) {
		w, path := open(t)
		_, err := w.Write(data[:100])
		require.NoError(t, err)
		injectCompletion(w.ring, 0, 60)
		injectCompletion(w.ring, 1, -int32(unix.ECANCELED))
		injectCompletion(w.ring, 0, -int32(unix.EOPNOTSUPP))
		injectCompletion(w.ring, 1, -int32(unix.ECANCELED))
		require.NoError(t, w.Close())
		requireOutput(t, path, 60, data[60:100])
	})
	t.Run("non-final", func(t *testing.T) {
		w, path := open(t)
		_, err := w.Write(data[:1<<20])
		require.NoError(t, err)
		injectCompletion(w.ring, 0, 1<<20-10)
		injectCompletion(w.ring, 0, -int32(unix.EOPNOTSUPP))
		_, err = w.Write(data[1<<20:])
		require.NoError(t, err)
		require.NoError(t, w.Close())
		requireOutput(t, path, 1<<20-10, data[1<<20-10:])
	})
	t.Run("fsync fails on retry", func(t *testing.T) {
		w, _ := open(t)
		_, err := w.Write(data[:100])
		require.NoError(t, err)
		injectCompletion(w.ring, 0, 60)
		injectCompletion(w.ring, 1, -int32(unix.ECANCELED))
		injectCompletion(w.ring, 0, 40)
		injectCompletion(w.ring, 1, -int32(unix.EIO))
		require.ErrorIs(t, w.Close(), unix.EIO)
	})
	t.Run("fsync fails beside short write", func(t *testing.T) {
		w, _ := open(t)
		_, err := w.Write(data[:100])
		require.NoError(t, err)
		injectCompletion(w.ring, 0, 60)
		injectCompletion(w.ring, 1, -int32(unix.EIO))
		injectCompletion(w.ring, 0, 40)
		injectCompletion(w.ring, 1, 0)
		require.ErrorIs(t, w.Close(), unix.EIO)
	})
	t.Run("fsync fails beside unsupported write", func(t *testing.T) {
		w, _ := open(t)
		_, err := w.Write(data[:100])
		require.NoError(t, err)
		injectCompletion(w.ring, 0, -int32(unix.EOPNOTSUPP))
		injectCompletion(w.ring, 1, -int32(unix.EIO))
		require.ErrorIs(t, w.Close(), unix.EIO)
	})
}

func TestWriterFallsBackWhenRingFsyncUnsupported(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "output"))
	require.NoError(t, err)
	w := NewWriter(f)
	require.NotNil(t, w.ring)
	_, err = w.Write([]byte("data"))
	require.NoError(t, err)
	injectCompletion(w.ring, 0, 4)
	injectCompletion(w.ring, 1, -int32(unix.EINVAL))
	require.NoError(t, w.Close())
}

func TestWriterSyncsAfterFallback(t *testing.T) {
	for _, ringFsync := range []bool{true, false} {
		t.Run(fmt.Sprint(ringFsync), func(t *testing.T) {
			pr, pw, err := os.Pipe()
			require.NoError(t, err)
			defer pr.Close()
			w := NewWriter(pw)
			require.NotNil(t, w.ring)
			w.err = nil
			if ringFsync {
				_, err = w.Write([]byte("data"))
				require.NoError(t, err)
				injectCompletion(w.ring, 0, 4)
				injectCompletion(w.ring, 1, -int32(unix.EINVAL))
			} else {
				w.ring.Close()
				w.ring = nil
			}
			require.ErrorIs(t, w.Close(), unix.EINVAL)
		})
	}
}

func TestWriterFsyncDrainsEarlierWrite(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "output"))
	require.NoError(t, err)
	w := NewWriter(f)
	require.NotNil(t, w.ring)
	defer func() {
		if w.ring != nil {
			w.ring.Close()
		}
		_ = f.Close()
	}()

	data := []byte("data")
	_, err = w.Write(data)
	require.NoError(t, err)
	injectCompletion(w.ring, 0, int32(len(data)))
	injectCompletion(w.ring, 1, 0)
	require.NoError(t, w.flush(true))

	flags := w.ring.sqes[sqeSize+1]
	require.Equal(t, uint8(1<<1), flags, "fsync SQE flags: got %d", flags)
}

func TestWriterFallsBackAfterRingReset(t *testing.T) {
	t.Run("nothing in flight", func(t *testing.T) {
		f, err := os.Create(filepath.Join(t.TempDir(), "output"))
		require.NoError(t, err)
		w := NewWriter(f)
		require.NotNil(t, w.ring)
		t.Cleanup(func() {
			_ = f.Close()
		})
		previousEnter := ioUringEnter
		ioUringEnter = func(int, uint32, uint32, uint32) (uintptr, unix.Errno) {
			return 0, unix.EAGAIN
		}
		t.Cleanup(func() { ioUringEnter = previousEnter })

		data := []byte("data after ring reset")
		_, err = w.Write(data)
		require.NoError(t, err)

		require.NoError(t, w.flush(true))
		require.Nil(t, w.ring)
		actual, err := os.ReadFile(f.Name())
		require.NoError(t, err)
		require.Equal(t, data, actual)
	})

	t.Run("pending write", func(t *testing.T) {
		f, err := os.Create(filepath.Join(t.TempDir(), "output"))
		require.NoError(t, err)
		w := NewWriter(f)
		require.NotNil(t, w.ring)
		ringFD := w.ring.fd
		t.Cleanup(func() {
			_ = unix.Close(ringFD)
			_ = f.Close()
		})
		previousEnter := ioUringEnter
		calls := 0
		ioUringEnter = func(int, uint32, uint32, uint32) (uintptr, unix.Errno) {
			calls++
			if calls == 1 {
				return 1, 0
			}
			return 0, unix.EAGAIN
		}
		t.Cleanup(func() { ioUringEnter = previousEnter })

		data := []byte("data after partial submit")
		_, err = w.Write(data)
		require.NoError(t, err)

		err = w.flush(true)
		require.ErrorIs(t, err, unix.EAGAIN)
		var pathErr *os.PathError
		require.ErrorAs(t, err, &pathErr)
		require.Equal(t, f.Name(), pathErr.Path)
		require.Nil(t, w.ring)
		_, statErr := unix.FcntlInt(uintptr(ringFD), unix.F_GETFD, 0)
		require.NoError(t, statErr)
		actual, err := os.ReadFile(f.Name())
		require.NoError(t, err)
		require.Empty(t, actual)
	})
}

func injectCompletion(r *Ring, id uint64, res int32) {
	tail := atomic.LoadUint32(r.cqTail)
	cqe := r.cqes + (tail&r.cqMask)*cqeSize
	*(*uint64)(unsafe.Pointer(&r.cqRing[cqe])) = id
	*p32(r.cqRing, cqe+8) = uint32(res)
	atomic.StoreUint32(r.cqTail, tail+1)
}
