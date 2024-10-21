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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/finality/flags"
	"github.com/erigontech/erigon/polygon/bor/finality/rawdb"
)

type milestone struct {
	finality[*rawdb.Milestone]

	LockedMilestoneNumber uint64              // Locked sprint number
	LockedMilestoneHash   common.Hash         //Hash for the locked endBlock
	Locked                bool                //
	LockedMilestoneIDs    map[string]struct{} //list of milestone ids

	FutureMilestoneList  map[uint64]common.Hash // Future Milestone list
	FutureMilestoneOrder []uint64               // Future Milestone Order
	MaxCapacity          int                    //Capacity of future Milestone list
}

type milestoneService interface {
	finalityService

	GetMilestoneIDsList() []string
	RemoveMilestoneID(milestoneId string)
	LockMutex(endBlockNum uint64) bool
	UnlockMutex(doLock bool, milestoneId string, endBlockNum uint64, endBlockHash common.Hash)
	UnlockSprint(endBlockNum uint64)
	ProcessFutureMilestone(num uint64, hash common.Hash)
}

var (
	//Metrics for collecting the whitelisted milestone number
	whitelistedMilestoneMeter = metrics.GetOrCreateGauge("chain_milestone_latest")

	//Metrics for collecting the future milestone number
	futureMilestoneMeter = metrics.GetOrCreateGauge("chain_milestone_future")

	//Metrics for collecting the length of the MilestoneIds map
	milestoneIdsLengthMeter = metrics.GetOrCreateGauge("chain_milestone_idslength")

	//Metrics for collecting the number of valid chains received
	milestoneChainMeter = metrics.GetOrCreateGauge("chain_milestone_isvalidchain")
)

// IsValidChain checks the validity of chain by comparing it
// against the local milestone entries
func (m *milestone) IsValidChain(currentHeader uint64, chain []*types.Header) bool {
	//Checking for the milestone flag
	if !flags.Milestone {
		return true
	}

	m.finality.RLock()
	defer m.finality.RUnlock()

	var isValid = false
	defer func() {
		if isValid {
			milestoneChainMeter.Inc()
		} else {
			milestoneChainMeter.Dec()
		}
	}()

	res := m.finality.IsValidChain(currentHeader, chain)

	if !res {
		isValid = false
		return false
	}

	if m.Locked && !m.IsReorgAllowed(chain, m.LockedMilestoneNumber, m.LockedMilestoneHash) {
		isValid = false
		return false
	}

	if !m.IsFutureMilestoneCompatible(chain) {
		isValid = false
		return false
	}

	isValid = true
	return true
}

func (m *milestone) Process(block uint64, hash common.Hash) {
	m.finality.Lock()
	defer m.finality.Unlock()

	m.finality.Process(block, hash)

	for i := 0; i < len(m.FutureMilestoneOrder); i++ {
		if m.FutureMilestoneOrder[i] <= block {
			m.dequeueFutureMilestone()
		} else {
			break
		}
	}

	whitelistedMilestoneMeter.SetUint64(block)

	m.UnlockSprint(block)
}

// LockMutex This function will Lock the mutex at the time of voting
func (m *milestone) LockMutex(endBlockNum uint64) bool {
	m.finality.Lock()

	if m.doExist && endBlockNum <= m.Number { //if endNum is less than whitelisted milestone, then we won't lock the sprint
		log.Debug("[bor] endBlockNumber is less than or equal to latesMilestoneNumber", "endBlock Number", endBlockNum, "LatestMilestone Number", m.Number)
		return false
	}

	if m.Locked && endBlockNum < m.LockedMilestoneNumber {
		log.Debug("[bor] endBlockNum is less than locked milestone number", "endBlock Number", endBlockNum, "Locked Milestone Number", m.LockedMilestoneNumber)
		return false
	}

	return true
}

// UnlockMutex This function will unlock the mutex locked in LockMutex
func (m *milestone) UnlockMutex(doLock bool, milestoneId string, endBlockNum uint64, endBlockHash common.Hash) {
	m.Locked = m.Locked || doLock

	if doLock {
		m.UnlockSprint(m.LockedMilestoneNumber)
		m.Locked = true
		m.LockedMilestoneHash = endBlockHash
		m.LockedMilestoneNumber = endBlockNum
		m.LockedMilestoneIDs[milestoneId] = struct{}{}
	}

	err := rawdb.WriteLockField(m.db, m.Locked, m.LockedMilestoneNumber, m.LockedMilestoneHash, m.LockedMilestoneIDs)
	if err != nil {
		log.Error("Error in writing lock data of milestone to db", "err", err)
	}

	milestoneIdsLengthMeter.SetInt(len(m.LockedMilestoneIDs))

	m.finality.Unlock()
}

// UnlockSprint This function will unlock the locked sprint
func (m *milestone) UnlockSprint(endBlockNum uint64) {
	if endBlockNum < m.LockedMilestoneNumber {
		return
	}

	if m.finality.TryLock() {
		defer m.finality.Unlock()
	}

	m.Locked = false

	m.purgeMilestoneIDsList()
	purgedMilestoneIDs := map[string]struct{}{}
	err := rawdb.WriteLockField(m.db, m.Locked, m.LockedMilestoneNumber, m.LockedMilestoneHash, purgedMilestoneIDs)

	if err != nil {
		log.Error("[bor] Error in writing lock data of milestone to db", "err", err)
	}
}

// RemoveMilestoneID This function will remove the stored milestoneID
func (m *milestone) RemoveMilestoneID(milestoneId string) {
	m.finality.Lock()
	defer m.finality.Unlock()

	delete(m.LockedMilestoneIDs, milestoneId)

	if len(m.LockedMilestoneIDs) == 0 {
		m.Locked = false
	}

	err := rawdb.WriteLockField(m.db, m.Locked, m.LockedMilestoneNumber, m.LockedMilestoneHash, m.LockedMilestoneIDs)

	if err != nil {
		log.Error("[bor] Error in writing lock data of milestone to db", "err", err)
	}

}

// IsReorgAllowed This will check whether the incoming chain matches the locked sprint hash
func (m *milestone) IsReorgAllowed(chain []*types.Header, lockedMilestoneNumber uint64, lockedMilestoneHash common.Hash) bool {
	if chain[len(chain)-1].Number.Uint64() <= lockedMilestoneNumber { //Can't reorg if the end block of incoming
		return false //chain is less than locked sprint number
	}

	for i := 0; i < len(chain); i++ {
		if chain[i].Number.Uint64() == lockedMilestoneNumber {
			return chain[i].Hash() == lockedMilestoneHash
		}
	}

	return true
}

// GetMilestoneIDsList This will return the list of milestoneIDs stored.
func (m *milestone) GetMilestoneIDsList() []string {
	m.finality.RLock()
	defer m.finality.RUnlock()

	// fixme: use generics :)
	keys := make([]string, 0, len(m.LockedMilestoneIDs))
	for key := range m.LockedMilestoneIDs {
		keys = append(keys, key)
	}

	return keys
}

// This is remove the milestoneIDs stored in the list.
func (m *milestone) purgeMilestoneIDsList() {
	// try is used here as the finality lock is preserved over calls - so the lock state
	// is not clearly defined in the local code - this likely needs to be revised
	if m.finality.TryLock() {
		defer m.finality.Unlock()
	}

	m.LockedMilestoneIDs = make(map[string]struct{})
}

func (m *milestone) IsFutureMilestoneCompatible(chain []*types.Header) bool {
	//Tip of the received chain
	chainTipNumber := chain[len(chain)-1].Number.Uint64()

	for i := len(m.FutureMilestoneOrder) - 1; i >= 0; i-- {
		//Finding out the highest future milestone number
		//which is less or equal to received chain tip
		if chainTipNumber >= m.FutureMilestoneOrder[i] {
			//Looking for the received chain 's particular block number(matching future milestone number)
			for j := len(chain) - 1; j >= 0; j-- {
				if chain[j].Number.Uint64() == m.FutureMilestoneOrder[i] {
					endBlockNum := m.FutureMilestoneOrder[i]
					endBlockHash := m.FutureMilestoneList[endBlockNum]

					//Checking the received chain matches with future milestone
					return chain[j].Hash() == endBlockHash
				}
			}
		}
	}

	return true
}

func (m *milestone) ProcessFutureMilestone(num uint64, hash common.Hash) {
	m.finality.Lock()
	defer m.finality.Unlock()

	if len(m.FutureMilestoneOrder) < m.MaxCapacity {
		m.enqueueFutureMilestone(num, hash)
	}

	if num < m.LockedMilestoneNumber {
		return
	}

	m.Locked = false
	m.purgeMilestoneIDsList()
	purgedMilestoneIDs := map[string]struct{}{}
	err := rawdb.WriteLockField(m.db, m.Locked, m.LockedMilestoneNumber, m.LockedMilestoneHash, purgedMilestoneIDs)

	if err != nil {
		log.Error("[bor] Error in writing lock data of milestone to db", "err", err)
	}
}

// EnqueueFutureMilestone add the future milestone to the list
func (m *milestone) enqueueFutureMilestone(key uint64, hash common.Hash) {
	if _, ok := m.FutureMilestoneList[key]; ok {
		log.Debug("[bor] Future milestone already exist", "endBlockNumber", key, "futureMilestoneHash", hash)
		return
	}

	log.Debug("[bor] Enqueing new future milestone", "endBlockNumber", key, "futureMilestoneHash", hash)

	m.FutureMilestoneList[key] = hash
	m.FutureMilestoneOrder = append(m.FutureMilestoneOrder, key)

	err := rawdb.WriteFutureMilestoneList(m.db, m.FutureMilestoneOrder, m.FutureMilestoneList)
	if err != nil {
		log.Error("[bor] Error in writing future milestone data to db", "err", err)
	}

	futureMilestoneMeter.SetUint64(key)
}

// DequeueFutureMilestone remove the future milestone entry from the list.
func (m *milestone) dequeueFutureMilestone() {
	delete(m.FutureMilestoneList, m.FutureMilestoneOrder[0])
	m.FutureMilestoneOrder = m.FutureMilestoneOrder[1:]

	err := rawdb.WriteFutureMilestoneList(m.db, m.FutureMilestoneOrder, m.FutureMilestoneList)
	if err != nil {
		log.Error("[bor] Error in writing future milestone data to db", "err", err)
	}
}
