// Copyright 2024 The Erigon Authors
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

package seg

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/c2h5oh/datasize"
)

//Reader and Writer - decorators on Getter and Compressor - which
//can auto-use Next/NextUncompressed and Write/AddUncompressedWord - based on `FileCompression` passed to constructor

// Maybe in future will add support of io.Reader/Writer interfaces to this decorators
// Maybe in future will merge decorators into it's parents

type Reader struct {
	*Getter
	nextValue bool            // if nextValue true then getter.Next() expected to return value
	c         FileCompression // compressed
}

func NewReader(g *Getter, c FileCompression) *Reader {
	return &Reader{Getter: g, c: c}
}

func (g *Reader) MatchPrefix(prefix []byte) bool {
	if g.c.Has(CompressKeys) {
		return g.Getter.MatchPrefix(prefix)
	}
	return g.Getter.MatchPrefixUncompressed(prefix)
}

func (g *Reader) MatchCmp(prefix []byte) int {
	if g.c.Has(CompressKeys) {
		return g.Getter.MatchCmp(prefix)
	}
	return g.Getter.MatchCmpUncompressed(prefix)
}

func (g *Reader) MadvNormal() MadvDisabler {
	g.d.MadvNormal()
	return g
}
func (g *Reader) DisableReadAhead() { g.d.DisableReadAhead() }
func (g *Reader) FileName() string  { return g.Getter.FileName() }
func (g *Reader) Next(buf []byte) ([]byte, uint64) {
	fl := CompressKeys
	if g.nextValue {
		fl = CompressVals
		g.nextValue = false
	} else {
		g.nextValue = true
	}

	if g.c.Has(fl) {
		return g.Getter.Next(buf)
	}
	return g.Getter.NextUncompressed()
}

func (g *Reader) Reset(offset uint64) {
	g.nextValue = false
	g.Getter.Reset(offset)
}
func (g *Reader) Skip() (uint64, int) {
	fl := CompressKeys
	if g.nextValue {
		fl = CompressVals
		g.nextValue = false
	} else {
		g.nextValue = true
	}

	if g.c.Has(fl) {
		return g.Getter.Skip()
	}
	return g.Getter.SkipUncompressed()

}

type Writer struct {
	*Compressor
	keyWritten bool
	c          FileCompression
}

func NewWriter(kv *Compressor, compress FileCompression) *Writer {
	if compress.Has(CompressKeys) {
		kv.featureFlagBitmask.Set(KeyCompressionEnabled)
	}
	if compress.Has(CompressVals) {
		kv.featureFlagBitmask.Set(ValCompressionEnabled)
	}
	return &Writer{kv, false, compress}
}

func (c *Writer) Write(word []byte) (n int, err error) {
	fl := CompressKeys
	if c.keyWritten {
		fl = CompressVals
		c.keyWritten = false
	} else {
		c.keyWritten = true
	}

	if c.c&fl != 0 {
		return len(word), c.Compressor.AddWord(word)
	}
	return len(word), c.Compressor.AddUncompressedWord(word)
}

func (c *Writer) ReadFrom(r *Reader) error {
	var v []byte
	for r.HasNext() {
		v, _ = r.Next(v[:0])
		if _, err := c.Write(v); err != nil {
			return err
		}
	}
	return nil
}

func (c *Writer) Close() {
	if c.Compressor != nil {
		c.Compressor.Close()
	}
}

func DetectCompressType(getter *Getter) (compressed FileCompression) {
	keyCompressed := func() (compressed bool) {
		defer func() {
			if rec := recover(); rec != nil {
				compressed = true
			}
		}()
		getter.Reset(0)
		for i := 0; i < 100; i++ {
			if getter.HasNext() {
				_, _ = getter.SkipUncompressed()
			}
			if getter.HasNext() {
				_, _ = getter.Skip()
			}
		}
		return compressed
	}()

	valCompressed := func() (compressed bool) {
		defer func() {
			if rec := recover(); rec != nil {
				compressed = true
			}
		}()
		getter.Reset(0)
		for i := 0; i < 100; i++ {
			if getter.HasNext() {
				_, _ = getter.Skip()
			}
			if getter.HasNext() {
				_, _ = getter.SkipUncompressed()
			}
		}
		return compressed
	}()
	getter.Reset(0)

	if keyCompressed {
		compressed |= CompressKeys
	}
	if valCompressed {
		compressed |= CompressVals
	}
	return compressed
}

// Decompressor2bufio reads words from a Decompressor in a background goroutine
// and returns a bufio.Reader producing uvarint-length-prefixed words.
// The returned cleanup function must be deferred by the caller.
func Decompressor2bufio(d *Decompressor) (*bufio.Reader, func()) {
	pr, pw := io.Pipe()
	go func() {
		wr := bufio.NewWriterSize(pw, int(128*datasize.MB))
		var numBuf [binary.MaxVarintLen64]byte
		g := d.MakeGetter()
		buf := make([]byte, 0, 1*datasize.MB)
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
			n := binary.PutUvarint(numBuf[:], uint64(len(buf)))
			if _, err := wr.Write(numBuf[:n]); err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := wr.Write(buf); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		wr.Flush()
		pw.Close()
	}()
	return bufio.NewReaderSize(pr, int(128*datasize.MB)), func() { pr.Close() }
}

// Bufio2compressor reads uvarint-length-prefixed words from src and writes them to a Writer.
// Optional wordFunc transforms each word before writing; return nil to skip writing the word.
func Bufio2compressor(ctx context.Context, src *bufio.Reader, w *Writer, wordFunc func(word []byte) ([]byte, error)) error {
	word := make([]byte, 0, int(1*datasize.MB))
	var l uint64
	var err error
	for l, err = binary.ReadUvarint(src); err == nil; l, err = binary.ReadUvarint(src) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if cap(word) < int(l) {
			word = make([]byte, l)
		} else {
			word = word[:l]
		}
		if _, err = io.ReadFull(src, word); err != nil {
			return err
		}
		if wordFunc != nil {
			word, err = wordFunc(word)
			if err != nil {
				return err
			}
			if word == nil {
				continue
			}
		}
		if _, err := w.Write(word); err != nil {
			return err
		}
	}
	if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
