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

package bor

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/rlp"
)

//go:generate mockgen -typed=true -destination=./state_receiver_mock.go -package=bor . StateReceiver
type StateReceiver interface {
	CommitState(event rlp.RawValue, syscall consensus.SystemCall) error
}

type ChainStateReceiver struct {
	contractAddress libcommon.Address
}

func NewStateReceiver(contractAddress string) *ChainStateReceiver {
	return &ChainStateReceiver{
		contractAddress: libcommon.HexToAddress(contractAddress),
	}
}

func (gc *ChainStateReceiver) CommitState(event rlp.RawValue, syscall consensus.SystemCall) error {
	_, err := syscall(gc.contractAddress, event)
	return err
}
