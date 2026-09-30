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
	"github.com/klauspost/compress/gzhttp/writer/gzkp"
	"github.com/klauspost/compress/gzhttp/writer/zstdkp"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// countingPool mirrors the per-level pools of gzhttp's default writers, which
// expose no hook to observe them. A single pool per encoder suffices because
// gzhttp always passes the level configured on the wrapper.
type countingPool[T any] struct {
	pool         sync.Pool
	hits, misses metrics.Counter
	inUse        metrics.Gauge
}

func (p *countingPool[T]) get(newWriter func() T) T {
	p.inUse.Inc()
	if w, ok := p.pool.Get().(T); ok {
		p.hits.Inc()
		return w
	}
	p.misses.Inc()
	return newWriter()
}

func (p *countingPool[T]) put(w T) {
	p.pool.Put(w)
	p.inUse.Dec()
}

var gzipWriters = countingPool[*gzip.Writer]{hits: gzipPoolHits, misses: gzipPoolMisses, inUse: gzipWritersInUse}

type pooledGzipWriter struct{ *gzip.Writer }

func (w *pooledGzipWriter) Close() error {
	if w.Writer == nil {
		return nil
	}
	err := w.Writer.Close()
	gzipWriters.put(w.Writer)
	w.Writer = nil
	return err
}

var gzipWriterFactory = writer.GzipWriterFactory{
	Levels: gzkp.Levels,
	New: func(w io.Writer, level int) writer.GzipWriter {
		gzw := gzipWriters.get(func() *gzip.Writer {
			gzw, _ := gzip.NewWriterLevel(nil, level) // level is within Levels, so no error
			return gzw
		})
		gzw.Reset(w)
		return &pooledGzipWriter{gzw}
	},
}

var zstdWriters = countingPool[*zstd.Encoder]{hits: zstdPoolHits, misses: zstdPoolMisses, inUse: zstdWritersInUse}

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

// zstdWriterFactory keeps the encoder options of gzhttp's default zstdkp
// writer: they set both the per-encoder memory and the output.
var zstdWriterFactory = writer.ZstdWriterFactory{
	Levels: zstdkp.Levels,
	New: func(w io.Writer, level int) writer.ZstdWriter {
		enc := zstdWriters.get(func() *zstd.Encoder {
			enc, _ := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.EncoderLevel(level)),
				zstd.WithEncoderConcurrency(1),
				zstd.WithLowerEncoderMem(true),
				zstd.WithWindowSize(128<<10),
			)
			return enc
		})
		enc.Reset(w)
		return &pooledZstdWriter{enc}
	},
}
