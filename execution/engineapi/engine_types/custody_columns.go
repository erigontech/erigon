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

package engine_types

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// CustodyColumnsSize is the byte-length of the custody columns bitarray (CELLS_PER_EXT_BLOB / 8 = 128 / 8 = 16).
const CustodyColumnsSize = 16

// CustodyColumns is a 16-byte bitarray where bit i indicates custody of blob column i.
// Defined by the Amsterdam Engine API specification for engine_forkchoiceUpdatedV4.
type CustodyColumns [CustodyColumnsSize]byte

func (c CustodyColumns) MarshalJSON() ([]byte, error) {
	return json.Marshal("0x" + hex.EncodeToString(c[:]))
}

func (c *CustodyColumns) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return err
	}
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	b, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("invalid custodyColumns hex: %w", err)
	}
	if len(b) != CustodyColumnsSize {
		return fmt.Errorf("invalid custodyColumns length: got %d bytes, want %d", len(b), CustodyColumnsSize)
	}
	copy(c[:], b)
	return nil
}
