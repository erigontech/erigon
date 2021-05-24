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

type OpInfo interface {
	opInfo()
}

// info used to check validity and use constant gas at JUMPDEST or JUMPI entry to a block
type BlockInfo struct {
	pc          uint64 // pc of block in contract code
	minStack    int    // min stack length for this block to not underflow
	maxStack    int    // max stack length for this block to not overflow
	constantGas uint64 // gas used by block - statically known
}

func (BlockInfo) opInfo() {}
func NewBlockInfo(pc uint64) *BlockInfo {
	p := new(BlockInfo)
	p.pc = pc
	return p
}

// destination for static jump
type JumpInfo struct {
	pc          uint64 // pc of jump in contract code
	dest uint64 // pc to jump to
}

func (JumpInfo) opInfo() {}
func NewJumpInfo(pc uint64, dest uint64) *JumpInfo {
	p := new(JumpInfo)
	p.pc = pc
	p.dest = dest
	return p
}

// decoded push data for PUSH*
type PushInfo struct {
	pc   uint64     // pc of push in contract code
	data uint256.Int
}

func (PushInfo) opInfo() {}
func NewPushInfo(pc uint64, data uint256.Int) *PushInfo {
	p := new(PushInfo)
	p.pc = pc
	p.data = data
	return p
}

// Will create one or more of the above for a basic block
func getBlockInfo(ctx *callCtx, pc uint64) (*BlockInfo, error) {
	contract := ctx.contract
	if contract.opsInfo == nil {
        contract.opsInfo = make([]OpInfo, len(contract.Code), len(contract.Code))
		info, err := analyzeBlock(ctx, 0)
		contract.preInfo = info
		return info, err
	} else if (int64(pc) == -1) {
		return contract.preInfo, nil
	} else if contract.opsInfo[pc] != nil {
		return contract.opsInfo[pc].(*BlockInfo), nil
	}
	info, err := analyzeBlock(ctx, pc)
	contract.opsInfo[pc] = info
	return info, err
}
