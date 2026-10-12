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

package pbt

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/execution/commitment"
)

func (t *Trie) EncodeCurrentState(buf []byte) ([]byte, error) {
	var root []byte
	if t.ctx != nil {
		var err error
		root, _, err = t.ctx.Branch(GlobalRootKey())
		if err != nil {
			return nil, err
		}
	}
	if len(root) > 65535 {
		return nil, fmt.Errorf("pbin: root record is %d bytes", len(root))
	}
	buf = append(buf, commitment.PBinStateMarker, commitment.PBinRowStateFormat, 0, 0, 0)
	binary.BigEndian.PutUint16(buf[3:5], uint16(len(root)))
	return append(buf, root...), nil
}

func (t *Trie) SetState(buf []byte) error {
	t.Reset()
	if len(buf) == 0 {
		return nil
	}
	if err := commitment.PBinValidateRowStateFormat(buf); err != nil {
		return err
	}
	if len(buf) < 5 {
		return fmt.Errorf("pbin: state header is %d bytes, want at least 5", len(buf))
	}
	rootLen := int(binary.BigEndian.Uint16(buf[3:5]))
	if len(buf) != 5+rootLen {
		return fmt.Errorf("pbin: state root record has %d bytes, %d present", rootLen, len(buf)-5)
	}
	if rootLen != 0 {
		if _, err := DecodeRecord(GlobalRootKey(), buf[5:]); err != nil {
			return err
		}
	}
	return nil
}
