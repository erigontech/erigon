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

//go:build !linux

package iouring

import (
	"bufio"
	"errors"
	"os"
)

type Writer struct {
	file   *os.File
	buf    *bufio.Writer
	err    error
	closed bool
}

func NewWriter(f *os.File) *Writer {
	return &Writer{file: f, buf: bufio.NewWriterSize(f, 1<<20)}
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, os.ErrClosed
	}
	return w.buf.Write(p)
}

func (w *Writer) Abort() {
	if !w.closed {
		w.closed = true
		_ = w.file.Close()
	}
}

func (w *Writer) Close() error {
	if !w.closed {
		w.closed = true
		w.err = errors.Join(w.buf.Flush(), w.file.Sync(), w.file.Close())
	}
	return w.err
}
