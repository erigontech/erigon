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

package state_accessors

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	historyRowPreambleSize = 14
	historyRowVersion      = byte(1)
)

var (
	historyRowMagic           = [4]byte{'G', 'L', 'O', 'A'}
	ErrHistoryRebuildRequired = errors.New("historical state row needs a rebuild")
)

func appendHistoryRowPreamble(dst []byte, kind byte) []byte {
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0)
	dst = append(dst, historyRowMagic[:]...)
	return append(dst, historyRowVersion, kind)
}

func parseHistoryRow(row []byte, format string) (byte, []byte, error) {
	if len(row) < historyRowPreambleSize {
		return 0, nil, rebuildRequired("row shorter than preamble")
	}
	if binary.LittleEndian.Uint32(row[:4]) != 0 || binary.LittleEndian.Uint32(row[4:8]) != 0 {
		return 0, nil, rebuildRequired("legacy row preamble")
	}
	if !bytes.Equal(row[8:12], historyRowMagic[:]) {
		return 0, nil, rebuildRequired("unknown row magic")
	}
	if row[12] != historyRowVersion {
		return 0, nil, rebuildRequired("unsupported %s version %d", format, row[12])
	}
	return row[13], row[historyRowPreambleSize:], nil
}

func rebuildRequired(reason string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrHistoryRebuildRequired, fmt.Sprintf(reason, args...))
}
