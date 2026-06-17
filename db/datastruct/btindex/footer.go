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

package btindex

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// btFooterMagic is the file's trailing 8 bytes: it identifies the format and proves the file isn't truncated.
var btFooterMagic = binary.BigEndian.Uint64([]byte("erigon\x00\x00"))

const (
	// btVersion is the bt format version (v1), stored in the footer anchor next to the magic so the layout can change later.
	btVersion = uint16(1)

	btMetadataLen = 24 // keys_count(8) + M(8) + ef_offset(8)
	btAnchorLen   = 16 // footer_len(4) + flags(2) + format_version(2) + magic(8)

	btEFAlign     = 4096 // EF section starts page-aligned for mmap/SIMD-friendly unsafe reads (nodes are byte-parsed, so left unaligned)
	btFooterAlign = 8
)

var errNotFooterFormat = errors.New("btindex: not a footer-format file")

// Metadata is the footer payload: what a reader needs that the body doesn't already carry.
type Metadata struct {
	KeysCount uint64
	M         uint64
	EfOffset  uint64 // byte offset of the EF section, so a reader can locate it without decoding nodes
}

// Footer store metadata at `Footer`: wins at write-once by append-only use-cases.
// Store metadata at `Header`: wins at forward-stream consumers (sockets, tar to tape), mutable
// page-addressed files (SQLite, Postgres, InnoDB — when mutate/extend file Footer will change its offset).
//
// `Footer` style file (for example `.bt`):
//
//	[ body ] # variable length. 4kb-alignment
//	[ footer: keys_count | M | ef_offset ] # variable length. 8-bytes-alignment
//	[ ANCHOR: footer_len:u32 | flags:u16 | format_version:u16 | magic:u64 ] # Fixed length. 8-bytes-alignment
//
// Build such files will be much more streaming-style-friendly. Don't need `etl` all incoming keys
// only to calculate `keys_count` (like we do now in `.bt`).
//
// On file open:
//   - Validate the `magic` first (right format + not truncated) -> fail-fast. Not u16 - because
//     probability of collision is high.
//   - `format_version` is next to the `magic` - then it will allow change ANCHOR format/len in the future.
type Footer struct {
	Meta          Metadata
	Flags         uint16
	FormatVersion uint16
}

// Encode writes the footer payload followed by the fixed anchor. The caller must
// have already padded so the footer starts 8-byte aligned.
func (f Footer) Encode(w io.Writer) error {
	var buf [btMetadataLen + btAnchorLen]byte
	binary.BigEndian.PutUint64(buf[0:8], f.Meta.KeysCount)
	binary.BigEndian.PutUint64(buf[8:16], f.Meta.M)
	binary.BigEndian.PutUint64(buf[16:24], f.Meta.EfOffset)
	binary.BigEndian.PutUint32(buf[24:28], uint32(btMetadataLen))
	binary.BigEndian.PutUint16(buf[28:30], f.Flags)
	binary.BigEndian.PutUint16(buf[30:32], f.FormatVersion)
	binary.BigEndian.PutUint64(buf[32:40], btFooterMagic)
	_, err := w.Write(buf[:])
	return err
}

// ReadFooter parses the footer from the end of data. It returns errNotFooterFormat when the
// trailing magic is absent (e.g. a legacy file), so the caller can fall back to legacy dispatch.
// footerStart is the byte offset where the footer begins (i.e. where the body ends).
func ReadFooter(data []byte) (f Footer, footerStart int, err error) {
	if len(data) < btAnchorLen {
		return Footer{}, 0, errNotFooterFormat
	}
	anchor := data[len(data)-btAnchorLen:]
	if binary.BigEndian.Uint64(anchor[8:16]) != btFooterMagic {
		return Footer{}, 0, errNotFooterFormat
	}
	footerLen := int(binary.BigEndian.Uint32(anchor[0:4]))
	footerStart = len(data) - btAnchorLen - footerLen
	if footerStart < 0 || footerLen < btMetadataLen {
		return Footer{}, 0, fmt.Errorf("btindex: corrupt footer (footer_len=%d, file=%d)", footerLen, len(data))
	}
	payload := data[footerStart : len(data)-btAnchorLen]
	return Footer{
		Meta: Metadata{
			KeysCount: binary.BigEndian.Uint64(payload[0:8]),
			M:         binary.BigEndian.Uint64(payload[8:16]),
			EfOffset:  binary.BigEndian.Uint64(payload[16:24]),
		},
		Flags:         binary.BigEndian.Uint16(anchor[4:6]),
		FormatVersion: binary.BigEndian.Uint16(anchor[6:8]),
	}, footerStart, nil
}

func alignUp[T ~int | ~uint64](n, a T) T { return (n + a - 1) &^ (a - 1) }

var alignPad [btEFAlign]byte

// countingWriter tracks the byte offset so sections can be padded to alignment.
type countingWriter struct {
	w       *bufio.Writer
	written uint64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.written += uint64(n)
	return n, err
}

func (c *countingWriter) Flush() error { return c.w.Flush() }

func (c *countingWriter) padTo(align int) error {
	pad := int(alignUp(c.written, uint64(align)) - c.written)
	if pad == 0 {
		return nil
	}
	_, err := c.Write(alignPad[:pad])
	return err
}
