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

package commitmentdb

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/stretchr/testify/require"
)

type bufferedContextBackend struct {
	branch      []byte
	branchStep  kv.Step
	account     *commitment.Update
	storage     *commitment.Update
	received    []bufferedBranchWrite
	putErr      error
	putErrAfter int
}

func (b *bufferedContextBackend) Branch([]byte) ([]byte, kv.Step, error) {
	return b.branch, b.branchStep, nil
}

func (b *bufferedContextBackend) Account([]byte) (*commitment.Update, error) {
	return b.account, nil
}

func (b *bufferedContextBackend) Storage([]byte) (*commitment.Update, error) {
	return b.storage, nil
}

func (b *bufferedContextBackend) PutBranch(prefix, data, prevData []byte) error {
	if b.putErr != nil && len(b.received) >= b.putErrAfter {
		return b.putErr
	}
	b.received = append(b.received, bufferedBranchWrite{
		prefix:   bytes.Clone(prefix),
		data:     bytes.Clone(data),
		prevData: bytes.Clone(prevData),
	})
	return nil
}

func TestBufferedPatriciaContextForwardsReadsAndDelaysWrites(t *testing.T) {
	backend := &bufferedContextBackend{
		branch:     []byte{1, 2},
		branchStep: 7,
		account:    &commitment.Update{Nonce: 3},
		storage:    &commitment.Update{StorageLen: 2},
	}
	ctx := NewBufferedPatriciaContext(backend)

	branch, step, err := ctx.Branch([]byte{0xaa})
	require.NoError(t, err)
	require.Equal(t, backend.branch, branch)
	require.Equal(t, backend.branchStep, step)

	account, err := ctx.Account([]byte{0xbb})
	require.NoError(t, err)
	require.Same(t, backend.account, account)
	storage, err := ctx.Storage([]byte{0xcc})
	require.NoError(t, err)
	require.Same(t, backend.storage, storage)

	prefix := []byte{1}
	data := []byte{2}
	prevData := []byte{3}
	require.NoError(t, ctx.PutBranch(prefix, data, prevData))
	require.Empty(t, backend.received)

	prefix[0] = 4
	data[0] = 5
	prevData[0] = 6
	require.NoError(t, ctx.Replay())
	require.Equal(t, []bufferedBranchWrite{{prefix: []byte{1}, data: []byte{2}, prevData: []byte{3}}}, backend.received)
	require.Empty(t, ctx.writes)
}

func TestBufferedPatriciaContextReplayPreservesOrderAndPrevData(t *testing.T) {
	directBackend := &bufferedContextBackend{}
	directWrites := []struct {
		prefix, data, prevData []byte
	}{
		{prefix: []byte{1}, data: []byte{2}, prevData: []byte{3}},
		{prefix: []byte{4}, data: []byte{5}, prevData: nil},
		{prefix: []byte{6}, data: []byte{}, prevData: []byte{7}},
	}
	for _, write := range directWrites {
		require.NoError(t, directBackend.PutBranch(write.prefix, write.data, write.prevData))
	}

	bufferedBackend := &bufferedContextBackend{}
	ctx := NewBufferedPatriciaContext(bufferedBackend)
	for _, write := range directWrites {
		require.NoError(t, ctx.PutBranch(write.prefix, write.data, write.prevData))
	}
	require.NoError(t, ctx.Replay())

	require.Equal(t, directBackend.received, bufferedBackend.received)
}
