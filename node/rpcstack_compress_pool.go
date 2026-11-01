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

package node

import (
	"bufio"
	"io"
	"sync"

	"github.com/klauspost/compress/gzhttp/writer"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

const (
	gzipLevel = gzip.BestSpeed
	zstdLevel = zstd.SpeedFastest
)

type writerPool[T any] struct {
	pool  sync.Pool
	inUse metrics.Gauge
}

func (p *writerPool[T]) get() T {
	p.inUse.Inc()
	return p.pool.Get().(T)
}

func (p *writerPool[T]) put(w T) {
	p.pool.Put(w)
	p.inUse.Dec()
}

// flate writes ~250-byte pieces, and net/http turns them into a write
// syscall per 4 KB.
const gzipOutBufSize = 64 * 1024

type bufferedGzipWriter struct {
	*gzip.Writer
	out *bufio.Writer
}

var gzipWriters = writerPool[*bufferedGzipWriter]{
	pool: sync.Pool{New: func() any {
		out := bufio.NewWriterSize(nil, gzipOutBufSize)
		gzw, _ := gzip.NewWriterLevel(out, gzipLevel) // valid constant level, so no error
		return &bufferedGzipWriter{gzw, out}
	}},
	inUse: gzipWritersInUse,
}

func (w *bufferedGzipWriter) Flush() error {
	if err := w.Writer.Flush(); err != nil {
		return err
	}
	return w.out.Flush()
}

type pooledGzipWriter struct{ *bufferedGzipWriter }

func (w *pooledGzipWriter) Close() error {
	if w.bufferedGzipWriter == nil {
		return nil
	}
	err := w.Writer.Close()
	if err == nil {
		err = w.out.Flush()
	}
	w.out.Reset(nil)
	gzipWriters.put(w.bufferedGzipWriter)
	w.bufferedGzipWriter = nil
	return err
}

var gzipWriterFactory = writer.GzipWriterFactory{
	Levels: func() (int, int) { return gzipLevel, gzipLevel },
	New: func(w io.Writer, _ int) writer.GzipWriter {
		bw := gzipWriters.get()
		bw.out.Reset(w)
		bw.Writer.Reset(bw.out)
		return &pooledGzipWriter{bw}
	},
}

// The encoder options are those of gzhttp's default zstdkp writer: they set
// both the per-encoder memory and the output.
var zstdWriters = writerPool[*zstd.Encoder]{
	pool: sync.Pool{New: func() any {
		enc, _ := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstdLevel),
			zstd.WithEncoderConcurrency(1),
			zstd.WithLowerEncoderMem(true),
			zstd.WithWindowSize(128<<10),
		)
		return enc
	}},
	inUse: zstdWritersInUse,
}

type pooledZstdWriter struct{ *zstd.Encoder }

func (w *pooledZstdWriter) Close() error {
	if w.Encoder == nil {
		return nil
	}
	err := w.Encoder.Close()
	w.Encoder.Reset(nil)
	zstdWriters.put(w.Encoder)
	w.Encoder = nil
	return err
}

var zstdWriterFactory = writer.ZstdWriterFactory{
	Levels: func() (int, int) { return int(zstdLevel), int(zstdLevel) },
	New: func(w io.Writer, _ int) writer.ZstdWriter {
		enc := zstdWriters.get()
		enc.Reset(w)
		return &pooledZstdWriter{enc}
	},
}
