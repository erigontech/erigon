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

package trie

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCreateLoadingPrefixes(t *testing.T) {
	assert := assert.New(t)

	tr := New(common.Hash{})
	kAcc1 := common.FromHex("0001cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c83")
	kInc := make([]byte, 8)
	binary.BigEndian.PutUint64(kInc, uint64(1))
	ks1 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000001")
	acc1 := accounts.NewAccount()
	acc1.Balance.SetUint64(12345)
	acc1.Incarnation = 1
	acc1.Initialised = true
	tr.UpdateAccount(kAcc1, &acc1)
	tr.Update(concat(kAcc1, ks1...), []byte{1, 2, 3})

	kAcc2 := common.FromHex("0002cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c83")
	ks2 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000001")
	ks22 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000002")
	acc2 := accounts.NewAccount()
	acc2.Balance.SetUint64(6789)
	acc2.Incarnation = 1
	acc2.Initialised = true
	tr.UpdateAccount(kAcc2, &acc2)
	tr.Update(concat(kAcc2, ks2...), []byte{4, 5, 6})
	tr.Update(concat(kAcc2, ks22...), []byte{7, 8, 9})
	tr.Hash()

	// Evict accounts only
	tr.EvictNode(keybytesToHex(kAcc1))
	tr.EvictNode(keybytesToHex(kAcc2))
	rs := NewRetainList(0)
	rs.AddKey(concat(concat(kAcc1, kInc...), ks1...))
	rs.AddKey(concat(concat(kAcc2, kInc...), ks2...))
	rs.AddKey(concat(concat(kAcc2, kInc...), ks22...))
	dbPrefixes, fixedbits, hooks := tr.FindSubTriesToLoad(rs)
	assert.Equal("[0001cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c830000000000000001 0002cf1ce0664746d39af9f6db99dc3370282f1d9d48df7f804b7e6499558c830000000000000001]", fmt.Sprintf("%x", dbPrefixes))
	assert.Equal("[320 320]", fmt.Sprintf("%d", fixedbits))
	assert.Equal("[000000010c0f010c0e000606040704060d03090a0f090f060d0b09090d0c030307000208020f010d090d04080d0f070f0800040b070e060409090505080c0803 000000020c0f010c0e000606040704060d03090a0f090f060d0b09090d0c030307000208020f010d090d04080d0f070f0800040b070e060409090505080c0803]", fmt.Sprintf("%x", hooks))

	// Evict everytning
	tr.EvictNode([]byte{})
	// if resolve only accounts
	rs = NewRetainList(0)
	rs.AddKey(kAcc1)
	rs.AddKey(kAcc2)
	dbPrefixes, fixedbits, hooks = tr.FindSubTriesToLoad(rs)
	assert.Equal("[]", fmt.Sprintf("%x", dbPrefixes))
	assert.Equal("[0]", fmt.Sprintf("%d", fixedbits))
	assert.Equal("[]", fmt.Sprintf("%x", hooks))
}
