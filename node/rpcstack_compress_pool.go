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

var gzipWriters = writerPool[*gzip.Writer]{
	pool: sync.Pool{New: func() any {
		gzw, _ := gzip.NewWriterLevel(nil, gzipLevel) // valid constant level, so no error
		return gzw
	}},
	inUse: gzipWritersInUse,
}

type pooledGzipWriter struct{ *gzip.Writer }

func (w *pooledGzipWriter) Close() error {
	if w.Writer == nil {
		return nil
	}
	err := w.Writer.Close()
	w.Writer.Reset(nil)
	gzipWriters.put(w.Writer)
	w.Writer = nil
	return err
}

var gzipWriterFactory = writer.GzipWriterFactory{
	Levels: func() (int, int) { return gzipLevel, gzipLevel },
	New: func(w io.Writer, _ int) writer.GzipWriter {
		gzw := gzipWriters.get()
		gzw.Reset(w)
		return &pooledGzipWriter{gzw}
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
