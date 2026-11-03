// Copyright 2022 The Erigon Authors
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

package state

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

func BenchmarkDomainFileCacheHit(b *testing.B) {
	dt := storageDomainTxWithFileKey(b, equalHashSalt, equalHashKeyA, common.FromHex("0xdeadbeef"))
	if _, found, _, _, err := dt.getLatestFromFiles(equalHashKeyA, nil, kv.NoStepBound); err != nil || !found {
		b.Fatal(found, err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, _, err := dt.getLatestFromFiles(equalHashKeyA, nil, kv.NoStepBound); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDomainFileCacheInsert(b *testing.B) {
	c := NewDomainGetFromFileCache(domainGetFromFileCacheLimit)
	v := common.FromHex("0xdeadbeef")
	b.ReportAllocs()
	for i := uint64(0); b.Loop(); i++ {
		c.add(equalHashKeyA, i*0x9E3779B97F4A7C15, domainGetFromFileCacheItem{lvl: 1, v: v})
	}
}
