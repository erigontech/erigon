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
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type pbinVerifyMemoryContext struct {
	records map[string][]byte
	reads   atomic.Int64
}

func (c *pbinVerifyMemoryContext) Branch(key []byte) ([]byte, kv.Step, error) {
	c.reads.Add(1)
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *pbinVerifyMemoryContext) PutBranch(key, data, _ []byte) error {
	if len(data) == 0 {
		delete(c.records, string(key))
		return nil
	}
	c.records[string(key)] = bytes.Clone(data)
	return nil
}

func (c *pbinVerifyMemoryContext) Account([]byte) (*commitment.Update, error) {
	return nil, nil
}

func (c *pbinVerifyMemoryContext) Storage([]byte) (*commitment.Update, error) {
	return nil, nil
}

func TestPBinVerifierWithoutRecordsDoesNotAllocateBucketKeyIndex(t *testing.T) {
	ctx := &pbinVerifyMemoryContext{records: make(map[string][]byte)}
	trie := NewTrie(ctx)
	verifier := trie.newVerifier()
	require.Nil(t, verifier.verifiedBucketKeys)
}
