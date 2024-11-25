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

// nolint
package rawdb

import (
	"errors"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
)

var (
	lastCheckpoint = []byte("LastCheckpoint")

	ErrEmptyLastFinality                    = errors.New("empty response while getting last finality")
	ErrIncorrectFinality                    = errors.New("last checkpoint in the DB is incorrect")
	ErrIncorrectFinalityToStore             = errors.New("failed to marshal the last finality struct")
	ErrDBNotResponding                      = errors.New("failed to store the last finality struct")
	ErrIncorrectLockFieldToStore            = errors.New("failed to marshal the lockField struct ")
	ErrIncorrectLockField                   = errors.New("lock field in the DB is incorrect")
	ErrIncorrectFutureMilestoneFieldToStore = errors.New("failed to marshal the future milestone field struct ")
	ErrIncorrectFutureMilestoneField        = errors.New("future milestone field  in the DB is incorrect")
)

type Checkpoint struct {
	Finality
}

func (c *Checkpoint) clone() *Checkpoint {
	return &Checkpoint{}
}

func (c *Checkpoint) block() (uint64, libcommon.Hash) {
	return c.Block, c.Hash
}
