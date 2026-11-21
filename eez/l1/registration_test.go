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

package l1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol"
)

// TestReadRegistration: the manager's threshold, and the key and signer of
// each proof system it registers; one with a zero key is left out unread.
func TestReadRegistration(t *testing.T) {
	registry, manager := common.Address{0x1}, common.Address{0x2}
	a, b := common.Address{0xa}, common.Address{0xb}
	signerA := common.Address{0x5a}
	word := func(b []byte) []byte { return common.LeftPadBytes(b, 32) }
	rollups := func(m common.Address) []byte { return append(word(m[:]), make([]byte, 64)...) }
	calls := func(m common.Address) []recording.CallResult {
		return []recording.CallResult{
			{Call: l1.Call{To: registry, Data: protocol.EncodeRollups(2)}, Return: rollups(m)},
			{Call: l1.Call{To: manager, Data: protocol.EncodeThreshold()}, Return: word([]byte{2})},
			{Call: l1.Call{To: manager, Data: protocol.EncodeVerificationKey(a)}, Return: word([]byte{0x1a})},
			{Call: l1.Call{To: manager, Data: protocol.EncodeVerificationKey(b)}, Return: word(nil)},
			{Call: l1.Call{To: a, Data: protocol.EncodeSigner()}, Return: word(signerA[:])},
		}
	}
	at := common.Hash{0xbb}
	c, err := recording.NewMemoryChain(1, &recording.Block{Header: l1.Header{Hash: at}, Calls: calls(manager)})
	require.NoError(t, err)
	reg, err := l1.ReadRegistration(t.Context(), c, registry, 2, []common.Address{a, b}, at)
	require.NoError(t, err)
	require.Equal(t, l1.Registration{Threshold: 2, Keys: map[common.Address]l1.Key{a: {VKey: common.BytesToHash([]byte{0x1a}), Signer: signerA}}}, reg)

	c, err = recording.NewMemoryChain(1, &recording.Block{Header: l1.Header{Hash: at}, Calls: calls(common.Address{})})
	require.NoError(t, err)
	_, err = l1.ReadRegistration(t.Context(), c, registry, 2, []common.Address{a, b}, at)
	require.ErrorIs(t, err, l1.ErrNoManager)
}
