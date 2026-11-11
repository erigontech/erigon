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

package protocol

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"slices"

	"github.com/erigontech/erigon/common"
)

// RollupConfig is the registry's record of a rollup, the return of
// rollups(rollupId); an unregistered rollup's record is zero.
type RollupConfig struct {
	RollupContract common.Address
	Root           common.Hash
	EtherBalance   *big.Int
}

// EncodeRollups returns the input of rollups(rollupID).
func EncodeRollups(rollupID uint64) []byte {
	var word common.Hash
	binary.BigEndian.PutUint64(word[24:], rollupID)
	return append(slices.Clone(rollupsMethod.ID), word[:]...)
}

// DecodeRollupsReturn decodes the output of rollups(rollupId). As
// Solidity's decoder, it ignores bytes past the record.
func DecodeRollupsReturn(out []byte) (*RollupConfig, error) {
	values, err := rollupsMethod.Outputs.Unpack(out)
	if err != nil {
		return nil, fmt.Errorf("rollups return: %w", err)
	}
	var c struct {
		RollupContract common.Address
		Root           [32]byte
		EtherBalance   *big.Int
	}
	if err := rollupsMethod.Outputs.Copy(&c, values); err != nil {
		return nil, fmt.Errorf("rollups return: %w", err)
	}
	return &RollupConfig{RollupContract: c.RollupContract, Root: c.Root, EtherBalance: c.EtherBalance}, nil
}
