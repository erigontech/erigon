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
	"bytes"
	"github.com/erigontech/erigon/db/kv"
)

type BranchWrite struct {
	PrevData []byte
	NewData  []byte
}

// Not safe for concurrent use: assumes Process runs single-threaded.
type RecordingContext struct {
	inner PatriciaContext

	branches    map[string][]byte
	accounts    map[string][]byte
	storages    map[string][]byte
	putBranches map[string]BranchWrite
}

func NewRecordingContext(inner PatriciaContext) *RecordingContext {
	return &RecordingContext{
		inner:       inner,
		branches:    make(map[string][]byte),
		accounts:    make(map[string][]byte),
		storages:    make(map[string][]byte),
		putBranches: make(map[string]BranchWrite),
	}
}

func (rc *RecordingContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	data, step, err := rc.inner.Branch(prefix)
	if err != nil {
		return data, step, err
	}
	if data != nil {
		rc.branches[string(bytes.Clone(prefix))] = bytes.Clone(data)
	}
	return data, step, nil
}

func (rc *RecordingContext) Account(plainKey []byte) (*Update, error) {
	u, err := rc.inner.Account(plainKey)
	if err != nil {
		return u, err
	}
	if u != nil {
		var numBuf [10]byte
		encoded := u.Encode(nil, numBuf[:])
		rc.accounts[string(bytes.Clone(plainKey))] = encoded
	}
	return u, nil
}

func (rc *RecordingContext) Storage(plainKey []byte) (*Update, error) {
	u, err := rc.inner.Storage(plainKey)
	if err != nil {
		return u, err
	}
	if u != nil {
		var numBuf [10]byte
		encoded := u.Encode(nil, numBuf[:])
		rc.storages[string(bytes.Clone(plainKey))] = encoded
	}
	return u, nil
}

func (rc *RecordingContext) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	rc.putBranches[string(bytes.Clone(prefix))] = BranchWrite{
		PrevData: bytes.Clone(prevData),
		NewData:  bytes.Clone(data),
	}
	return rc.inner.PutBranch(prefix, data, prevData)
}
