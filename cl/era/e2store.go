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

// Package era reads and writes beacon-chain .era files, the consensus-layer
// archival format defined at
// https://github.com/eth-clients/e2store-format-specs/blob/main/formats/era.md
//
// An .era file groups one era (up to SLOTS_PER_HISTORICAL_ROOT slots) of
// beacon blocks together with the BeaconState at the era boundary, framed as
// e2store TLV records.
package era

import (
	"encoding/binary"
	"fmt"
	"io"
)

// e2store record type tags. Stored on the wire as the literal 2-byte
// sequences below (not endianness-dependent — they are byte arrays).
var (
	TypeVersion         = [2]byte{0x65, 0x32} // "e2"
	TypeCompressedBlock = [2]byte{0x01, 0x00} // CompressedSignedBeaconBlock
	TypeCompressedState = [2]byte{0x02, 0x00} // CompressedBeaconState
	TypeSlotIndex       = [2]byte{0x69, 0x32} // SlotIndex
	TypeEmpty           = [2]byte{0x00, 0x00}
)

// e2storeHeaderLen is the fixed record header size: type[2] + length[6 LE].
const e2storeHeaderLen = 8

// e2storeRecord is one TLV record: a 2-byte type tag and its payload.
type e2storeRecord struct {
	typ  [2]byte
	data []byte
}

// readE2storeRecord reads one TLV record from r. It returns io.EOF when r is
// positioned exactly at end-of-stream before the header.
func readE2storeRecord(r io.Reader) (e2storeRecord, error) {
	var hdr [e2storeHeaderLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return e2storeRecord{}, err // io.EOF / io.ErrUnexpectedEOF propagate
	}
	rec := e2storeRecord{typ: [2]byte{hdr[0], hdr[1]}}

	// length is a little-endian uint48 in hdr[2:8].
	var lenBuf [8]byte
	copy(lenBuf[:6], hdr[2:8])
	length := binary.LittleEndian.Uint64(lenBuf[:])

	rec.data = make([]byte, length)
	if _, err := io.ReadFull(r, rec.data); err != nil {
		return e2storeRecord{}, fmt.Errorf("era: truncated record body (type %x, want %d bytes): %w", rec.typ, length, err)
	}
	return rec, nil
}

// writeE2storeRecord writes one TLV record to w and returns the number of
// bytes written (header + payload).
func writeE2storeRecord(w io.Writer, typ [2]byte, data []byte) (int, error) {
	var hdr [e2storeHeaderLen]byte
	hdr[0], hdr[1] = typ[0], typ[1]

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(data)))
	copy(hdr[2:8], lenBuf[:6])
	if lenBuf[6] != 0 || lenBuf[7] != 0 {
		return 0, fmt.Errorf("era: record length %d exceeds uint48", len(data))
	}

	if _, err := w.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(data); err != nil {
		return e2storeHeaderLen, err
	}
	return e2storeHeaderLen + len(data), nil
}
