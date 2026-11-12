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

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/dbg"
)

var dontCacheWrites = dbg.EnvBool("FILE_BUILD_DONTCACHE", true)

type uncachedWriter struct {
	f           *os.File
	unsupported bool
}

func UncachedWriter(f *os.File) io.Writer {
	if !dontCacheWrites {
		return f
	}
	return &uncachedWriter{f: f}
}

func (w *uncachedWriter) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) && !w.unsupported {
		n, err := unix.Pwritev2(int(w.f.Fd()), [][]byte{p[written:]}, -1, unix.RWF_DONTCACHE)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			w.unsupported = true
			break
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
		written += n
	}
	if written == len(p) {
		return written, nil
	}
	n, err := w.f.Write(p[written:])
	return written + n, err
}
