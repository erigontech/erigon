// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"github.com/holiman/uint256"
)

// we are creating an 

type OpInfo interface {
	opInfo()
}

// info used to check validity and use constant gas at JUMPDEST or JUMPI entry to a block
type BlockInfo struct {
	pc uint64			// pc of block in contract code
	minStack int		// min stack length for this block to not underflow
	maxStack int		// max stack length for this block to not overflow
	constantGas uint64	// gas used by block - statically known
}
func (BlockInfo) opInfo() {}
func NewBlockInfo(c *Contract, pc uint64) *BlockInfo {
	p := new(BlockInfo)
	p.pc = pc
	c.opsInfo[pc] = p
	return p
}

// destination for static jump
type JumpInfo struct {
	dest uint64	// pc to jump to
}
func (JumpInfo) opInfo() {}
func NewJumpInfo(c *Contract, pc uint64) *JumpInfo {
	p := new(JumpInfo)
	p.dest = pc
	c.opsInfo[pc] = p
	return p
}

// decoded push data for PUSH*
type PushInfo struct {
	data uint256.Int
}
func (PushInfo) opInfo() {}
func NewPushInfo(c *Contract, pc uint64, data uint256.Int) *PushInfo {
	p := new(PushInfo)
	p.data = data
	c.opsInfo[pc] = p
	return p
}

func getBlockInfo(ctx *callCtx, pc uint64) (*BlockInfo, error) {
	info := (ctx.contract.opsInfo[pc])
	if info != nil {
	    return info.(*BlockInfo), nil
	}
	return analyzeBlock(ctx, pc)
}




