// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package enr

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/rlp"
)

func BenchmarkDecodeRecord(b *testing.B) {
	var r Record
	r.Set(IPv4{192, 0, 2, 1})
	r.Set(TCP(30303))
	r.Set(UDP(30303))
	r.Set(IPv6{0x20, 0x01, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	r.Set(TCP6(30303))
	r.Set(UDP6(30303))
	r.Set(WithEntry("eth", []byte{0xc7, 0xc6, 0x84, 0xa0, 0x0b, 0xc6, 0x80}))
	r.Set(WithEntry("attnets", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}))
	require.NoError(b, signTest([]byte{5}, &r))

	enc, err := rlp.EncodeToBytes(&r)
	require.NoError(b, err)
	b.Logf("record size %d bytes, %d pairs", len(enc), len(r.pairs))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var dec Record
		if err := rlp.DecodeBytes(enc, &dec); err != nil {
			b.Fatal(err)
		}
	}
}
