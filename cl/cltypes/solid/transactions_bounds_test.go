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

package solid

import (
	"testing"

	"github.com/erigontech/erigon/common/ssz"
)

func TestTransactionsSSZ_DecodeSSZ_BoundsCheck(t *testing.T) {
	tests := []struct {
		name    string
		buf     []byte
		wantErr error
	}{
		{
			// PoC from security#66: 4 bytes [0x08,0x00,0x00,0x00]
			// DecodeOffset = 8, length = 8/4 = 2 (implies 2 txs)
			// But buffer is only 4 bytes — reading offset[1] at buf[4:8] would panic.
			name:    "security#66 crash payload - offset implies 2 txs in 4-byte buffer",
			buf:     []byte{0x08, 0x00, 0x00, 0x00},
			wantErr: ssz.ErrLowBufferSize,
		},
		{
			name:    "empty buffer",
			buf:     []byte{},
			wantErr: nil,
		},
		{
			name:    "too short for first offset",
			buf:     []byte{0x01, 0x02},
			wantErr: ssz.ErrLowBufferSize,
		},
		{
			name:    "zero transactions",
			buf:     []byte{0x00, 0x00, 0x00, 0x00},
			wantErr: nil,
		},
		{
			name:    "valid single tx",
			buf:     []byte{0x04, 0x00, 0x00, 0x00, 0xAA, 0xBB},
			wantErr: nil,
		},
		{
			// offset = 12 → length = 3, needs 12 bytes for offset array but only 8 given
			name:    "offset array exceeds buffer",
			buf:     []byte{0x0c, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff},
			wantErr: ssz.ErrLowBufferSize,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txs := &TransactionsSSZ{}
			err := txs.DecodeSSZ(tt.buf, 0)
			if err != tt.wantErr {
				t.Errorf("DecodeSSZ() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
