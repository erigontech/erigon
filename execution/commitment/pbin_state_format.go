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

package commitment

import (
	"encoding/binary"
	"fmt"
)

const PBinRowStateFormat byte = 0x20

const PBinStateMarker byte = 0xB1

func IsPBinState(buf []byte) bool { return len(buf) > 0 && buf[0] == PBinStateMarker }

func PBinValidateRowStateFormat(buf []byte) error {
	if len(buf) < 2 || !IsPBinState(buf) {
		return fmt.Errorf("pbin: state requires rebuild: not a pbin blob")
	}
	if buf[1] != PBinRowStateFormat {
		return fmt.Errorf("pbin: state requires rebuild: format %d, want %d", buf[1], PBinRowStateFormat)
	}
	if len(buf) < 5 {
		return fmt.Errorf("pbin: state requires rebuild: header is %d bytes, want at least 5", len(buf))
	}
	rootLen := int(binary.BigEndian.Uint16(buf[3:5]))
	if len(buf) != 5+rootLen {
		return fmt.Errorf("pbin: state requires rebuild: root record has %d bytes, %d present", rootLen, len(buf)-5)
	}
	return nil
}
