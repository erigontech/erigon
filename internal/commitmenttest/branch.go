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

package commitmenttest

import (
	"bytes"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

type MapBranchStore struct {
	Records map[string][]byte
}

func NewMapBranchStore() MapBranchStore {
	return MapBranchStore{Records: make(map[string][]byte)}
}

func (c *MapBranchStore) Branch(key []byte) ([]byte, kv.Step, error) {
	return bytes.Clone(c.Records[string(key)]), 0, nil
}

func (c *MapBranchStore) PutBranch(key, data, prev []byte) error {
	if !bytes.Equal(c.Records[string(key)], prev) {
		return fmt.Errorf("previous record mismatch for %x", key)
	}
	if len(data) == 0 {
		delete(c.Records, string(key))
	} else {
		c.Records[string(key)] = bytes.Clone(data)
	}
	return nil
}
