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

package v4

import (
	"bytes"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type mockContext struct {
	branches     map[string][]byte
	branchBuf    []byte
	branchCalls  [][]byte
	putCalls     int
	accountCalls int
	storageCalls int
}

func newMockContext() *mockContext {
	return &mockContext{branches: make(map[string][]byte)}
}

func (m *mockContext) Branch(key []byte) ([]byte, kv.Step, error) {
	m.branchCalls = append(m.branchCalls, bytes.Clone(key))
	data, ok := m.branches[string(key)]
	if !ok {
		return nil, 0, nil
	}
	if m.branchBuf == nil {
		m.branchBuf = make([]byte, 0, len(data))
	}
	m.branchBuf = append(m.branchBuf[:0], data...)
	return m.branchBuf, 0, nil
}

func (m *mockContext) PutBranch(key, data, _ []byte) error {
	m.putCalls++
	m.branches[string(key)] = bytes.Clone(data)
	return nil
}

func (m *mockContext) Account([]byte) (*commitment.Update, error) {
	m.accountCalls++
	return &commitment.Update{}, nil
}

func (m *mockContext) Storage([]byte) (*commitment.Update, error) {
	m.storageCalls++
	return &commitment.Update{}, nil
}

var _ commitment.PatriciaContext = (*mockContext)(nil)

type phaseAInput struct {
	hashedKey []byte
	plainKey  []byte
	update    *commitment.Update
}

func partition(stream []phaseAInput) ([]storageTask, []accountEntry) {
	p := newPartitioner()
	for _, item := range stream {
		if err := p.add(item.hashedKey, item.plainKey, item.update); err != nil {
			panic(err)
		}
	}
	return p.done()
}
