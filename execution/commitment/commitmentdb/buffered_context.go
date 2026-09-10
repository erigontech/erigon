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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type bufferedBranchWrite struct {
	prefix   []byte
	data     []byte
	prevData []byte
}

type BufferedPatriciaContext struct {
	inner  commitment.PatriciaContext
	writes []bufferedBranchWrite
}

func NewBufferedPatriciaContext(inner commitment.PatriciaContext) *BufferedPatriciaContext {
	return &BufferedPatriciaContext{inner: inner}
}

func (c *BufferedPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	return c.inner.Branch(prefix)
}

func (c *BufferedPatriciaContext) Account(plainKey []byte) (*commitment.Update, error) {
	return c.inner.Account(plainKey)
}

func (c *BufferedPatriciaContext) Storage(plainKey []byte) (*commitment.Update, error) {
	return c.inner.Storage(plainKey)
}

func (c *BufferedPatriciaContext) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	c.writes = append(c.writes, bufferedBranchWrite{
		prefix:   bytes.Clone(prefix),
		data:     bytes.Clone(data),
		prevData: bytes.Clone(prevData),
	})
	return nil
}

func (c *BufferedPatriciaContext) Replay() error {
	for i := range c.writes {
		write := &c.writes[i]
		if err := c.inner.PutBranch(write.prefix, write.data, write.prevData); err != nil {
			return err
		}
	}
	c.writes = nil
	return nil
}

var _ commitment.PatriciaContext = (*BufferedPatriciaContext)(nil)
