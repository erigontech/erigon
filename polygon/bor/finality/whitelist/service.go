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

package whitelist

import (
	"errors"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor/finality/rawdb"
)

var (
	ErrMismatch = errors.New("mismatch error")
	ErrNoRemote = errors.New("remote peer doesn't have a target block number")
)

type Service struct {
	checkpointService
	milestoneService
}

var ws *Service

func RegisterService(db kv.RwDB) {
	ws = NewService(db)
}

func GetWhitelistingService() *Service {
	return ws
}

func NewService(db kv.RwDB) *Service {
	var checkpointDoExist = true
	checkpointNumber, checkpointHash, err := rawdb.ReadFinality[*rawdb.Checkpoint](db)

	if err != nil {
		checkpointDoExist = false
	}

	var milestoneDoExist = true

	milestoneNumber, milestoneHash, err := rawdb.ReadFinality[*rawdb.Milestone](db)
	if err != nil {
		milestoneDoExist = false
	}

	locked, lockedMilestoneNumber, lockedMilestoneHash, lockedMilestoneIDs, err := rawdb.ReadLockField(db)
	if err != nil || !locked {
		locked = false
		lockedMilestoneIDs = make(map[string]struct{})
	}

	order, list, err := rawdb.ReadFutureMilestoneList(db)
	if err != nil {
		order = make([]uint64, 0)
		list = make(map[uint64]common.Hash)
	}

	return &Service{
		&checkpoint{
			finality[*rawdb.Checkpoint]{
				doExist:  checkpointDoExist,
				Number:   checkpointNumber,
				Hash:     checkpointHash,
				interval: 256,
				db:       db,
			},
		},

		&milestone{
			finality: finality[*rawdb.Milestone]{
				doExist:  milestoneDoExist,
				Number:   milestoneNumber,
				Hash:     milestoneHash,
				interval: 256,
				db:       db,
			},

			Locked:                locked,
			LockedMilestoneNumber: lockedMilestoneNumber,
			LockedMilestoneHash:   lockedMilestoneHash,
			LockedMilestoneIDs:    lockedMilestoneIDs,
			FutureMilestoneList:   list,
			FutureMilestoneOrder:  order,
			MaxCapacity:           10,
		},
	}
}

func (s *Service) PurgeWhitelistedCheckpoint() error {
	s.checkpointService.Purge()
	return nil
}

func (s *Service) PurgeWhitelistedMilestone() error {
	s.milestoneService.Purge()
	return nil
}

func (s *Service) GetWhitelistedCheckpoint() (bool, uint64, common.Hash) {
	return s.checkpointService.Get()
}

func (s *Service) GetWhitelistedMilestone() (bool, uint64, common.Hash) {
	return s.milestoneService.Get()
}

func (s *Service) ProcessMilestone(endBlockNum uint64, endBlockHash common.Hash) {
	s.milestoneService.Process(endBlockNum, endBlockHash)
}

func (s *Service) ProcessCheckpoint(endBlockNum uint64, endBlockHash common.Hash) {
	s.checkpointService.Process(endBlockNum, endBlockHash)
}

func (s *Service) IsValidChain(currentHeader uint64, chain []*types.Header) bool {
	checkpointBool := s.checkpointService.IsValidChain(currentHeader, chain)
	if !checkpointBool {
		return checkpointBool
	}

	milestoneBool := s.milestoneService.IsValidChain(currentHeader, chain)
	if !milestoneBool {
		return milestoneBool
	}

	return true
}

func (s *Service) GetMilestoneIDsList() []string {
	return s.milestoneService.GetMilestoneIDsList()
}

func splitChain(current uint64, chain []*types.Header) ([]*types.Header, []*types.Header) {
	var (
		pastChain   []*types.Header
		futureChain []*types.Header
		first       = chain[0].Number.Uint64()
		last        = chain[len(chain)-1].Number.Uint64()
	)

	if current >= first {
		if len(chain) == 1 || current >= last {
			pastChain = chain
		} else {
			pastChain = chain[:current-first+1]
		}
	}

	if current < last {
		if len(chain) == 1 || current < first {
			futureChain = chain
		} else {
			futureChain = chain[current-first+1:]
		}
	}

	return pastChain, futureChain
}

func isValidChain(currentHeader uint64, chain []*types.Header, doExist bool, number uint64, hash common.Hash, interval uint64) bool {
	// Check if we have milestone to validate incoming chain in memory
	if !doExist {
		// We don't have any entry, no additional validation will be possible
		return true
	}

	// Check if imported chain is less than whitelisted number
	if chain[len(chain)-1].Number.Uint64() < number {
		if currentHeader >= number { //If current tip of the chain is greater than whitelist number then return false
			return false
		} else {
			return true
		}
	}

	// Split the chain into past and future chain
	pastChain, _ := splitChain(currentHeader, chain)

	// Iterate over the chain and validate against the last milestone
	// It will handle all cases when the incoming chain has atleast one milestone
	for i := len(pastChain) - 1; i >= 0; i-- {
		if pastChain[i].Number.Uint64() == number {
			res := pastChain[i].Hash() == hash

			return res
		}
	}

	return true
}
