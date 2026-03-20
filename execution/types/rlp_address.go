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

package types

import (
	"fmt"
	"io"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

// EncodeOptionalAddress encodes an optional 20-byte address via w.
func EncodeOptionalAddress(addr *common.Address, w io.Writer, buffer []byte) error {
	if addr == nil {
		buffer[0] = rlp.EmptyStringCode
		_, err := w.Write(buffer[:1])
		return err
	}
	buffer[0] = rlp.EmptyStringCode + 20
	copy(buffer[1:21], addr[:])
	_, err := w.Write(buffer[:21])
	return err
}

// DecodeOptionalAddress decodes an optional 20-byte address from the RLP stream.
func DecodeOptionalAddress(dst **common.Address, s *rlp.Stream) error {
	kind, size, err := s.Kind()
	if err != nil {
		return err
	}
	switch {
	case kind == rlp.String && size == 0:
		*dst = nil
		return s.ReadBytes(nil)
	case kind == rlp.String && size == 20:
		*dst = &common.Address{}
		return s.ReadBytes((*dst)[:])
	case kind == rlp.List:
		return fmt.Errorf("expected string for address, got list")
	case kind == rlp.Byte:
		return fmt.Errorf("wrong size for address: 1")
	default:
		return fmt.Errorf("wrong size for address: %d", size)
	}
}
