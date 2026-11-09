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

package vm

import (
	"errors"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// blockContextPrecompile is a precompile whose output depends on the block. The
// EVM binds it to its block context before running it.
type blockContextPrecompile interface {
	withBlockContext(ctx *evmtypes.BlockContext) PrecompiledContract
}

// milliTimestamp returns the block time in milliseconds as a 32-byte word (BEP-706).
// Calldata is ignored.
type milliTimestamp struct{}

func (c *milliTimestamp) RequiredGas(input []byte) uint64 { return params.MilliTimestampGas }

func (c *milliTimestamp) Name() string { return "MilliTimestamp" }

func (c *milliTimestamp) Run(input []byte) ([]byte, error) {
	return nil, errors.New("milliTimestamp: needs the block context")
}

func (c *milliTimestamp) withBlockContext(ctx *evmtypes.BlockContext) PrecompiledContract {
	return &boundMilliTimestamp{millis: ctx.Time*1000 + uint64(ctx.Milliseconds)}
}

type boundMilliTimestamp struct {
	milliTimestamp
	millis uint64
}

func (c *boundMilliTimestamp) Run(input []byte) ([]byte, error) {
	out := uint256.NewInt(c.millis).Bytes32()
	return out[:], nil
}
