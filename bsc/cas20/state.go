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

package cas20

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// StateDB is the state CAS20 reads and writes. A read failure does not show up
// in the results; Err reports the first one.
type StateDB interface {
	GetCodeHash(addr common.Address) common.Hash
	GetCode(addr common.Address) []byte
	SetCode(addr common.Address, code []byte, reason tracing.CodeChangeReason)
	AddLog(log *types.Log)
	AddressInAccessList(addr common.Address) bool
	AddAddressToAccessList(addr common.Address)
	SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool)
	AddSlotToAccessList(addr common.Address, slot common.Hash)
	GetState(addr common.Address, slot common.Hash) common.Hash
	GetStateAndCommittedState(addr common.Address, slot common.Hash) (current, committed common.Hash)
	SetState(addr common.Address, slot, value common.Hash)
	AddRefund(gas uint64)
	SubRefund(gas uint64)
	Err() error
}

type ibsState struct {
	ibs *state.IntraBlockState
	err error
}

func newIBSState(ibs *state.IntraBlockState) *ibsState { return &ibsState{ibs: ibs} }

func (s *ibsState) keep(err error) {
	if err != nil && s.err == nil {
		s.err = err
	}
}

func (s *ibsState) Err() error { return s.err }

func (s *ibsState) GetCodeHash(addr common.Address) common.Hash {
	h, err := s.ibs.GetCodeHash(accounts.InternAddress(addr))
	s.keep(err)
	return h.Value()
}

func (s *ibsState) GetCode(addr common.Address) []byte {
	code, err := s.ibs.GetCode(accounts.InternAddress(addr))
	s.keep(err)
	return code
}

func (s *ibsState) SetCode(addr common.Address, code []byte, reason tracing.CodeChangeReason) {
	s.keep(s.ibs.SetCode(accounts.InternAddress(addr), code, reason))
}

func (s *ibsState) AddLog(log *types.Log) { s.ibs.AddLog(log) }

func (s *ibsState) AddressInAccessList(addr common.Address) bool {
	return s.ibs.AddressInAccessList(accounts.InternAddress(addr))
}

func (s *ibsState) AddAddressToAccessList(addr common.Address) {
	s.ibs.AddAddressToAccessList(accounts.InternAddress(addr))
}

func (s *ibsState) SlotInAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	return s.ibs.SlotInAccessList(accounts.InternAddress(addr), accounts.InternKey(slot))
}

func (s *ibsState) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	s.ibs.AddSlotToAccessList(accounts.InternAddress(addr), accounts.InternKey(slot))
}

func (s *ibsState) GetState(addr common.Address, slot common.Hash) common.Hash {
	v, err := s.ibs.GetState(accounts.InternAddress(addr), accounts.InternKey(slot))
	s.keep(err)
	return v.Bytes32()
}

func (s *ibsState) GetStateAndCommittedState(addr common.Address, slot common.Hash) (common.Hash, common.Hash) {
	a, k := accounts.InternAddress(addr), accounts.InternKey(slot)
	current, err := s.ibs.GetState(a, k)
	s.keep(err)
	committed, err := s.ibs.GetCommittedState(a, k)
	s.keep(err)
	return current.Bytes32(), committed.Bytes32()
}

func (s *ibsState) SetState(addr common.Address, slot, value common.Hash) {
	var v uint256.Int
	v.SetBytes32(value[:])
	s.keep(s.ibs.SetState(accounts.InternAddress(addr), accounts.InternKey(slot), v))
}

func (s *ibsState) AddRefund(gas uint64) { s.ibs.AddRefund(gas) }

func (s *ibsState) SubRefund(gas uint64) { s.keep(s.ibs.SubRefund(gas)) }
