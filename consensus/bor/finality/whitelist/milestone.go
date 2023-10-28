package whitelist

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/flags"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
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
	whitelistedMilestoneMeter = metrics.GetOrCreateCounter("chain_milestone_latest", true)

	//Metrics for collecting the future milestone number
	FutureMilestoneMeter = metrics.GetOrCreateCounter("chain_milestone_future", true)

	//Metrics for collecting the length of the MilestoneIds map
	MilestoneIdsLengthMeter = metrics.GetOrCreateCounter("chain_milestone_idslength", true)

	//Metrics for collecting the number of valid chains received
	MilestoneChainMeter = metrics.GetOrCreateCounter("chain_milestone_isvalidchain")
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

	var isValid bool = false
	defer func() {
		if isValid {
			MilestoneChainMeter.Add(1)
		} else {
			MilestoneChainMeter.Add(-1)
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

	whitelistedMilestoneMeter.Set(block)

	m.UnlockSprint(block)
}

// This function will Lock the mutex at the time of voting
func (m *milestone) LockMutex(endBlockNum uint64) bool {
	m.finality.Lock()

	if m.doExist && endBlockNum <= m.Number { //if endNum is less than whitelisted milestone, then we won't lock the sprint
		log.Debug("endBlockNumber is less than or equal to latesMilestoneNumber", "endBlock Number", endBlockNum, "LatestMilestone Number", m.Number)
		return false
	}

	if m.Locked && endBlockNum < m.LockedMilestoneNumber {
		log.Debug("endBlockNum is less than locked milestone number", "endBlock Number", endBlockNum, "Locked Milestone Number", m.LockedMilestoneNumber)
		return false
	}

	return true
}

// This function will unlock the mutex locked in LockMutex
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

	milestoneIDLength := uint64(len(m.LockedMilestoneIDs))
	MilestoneIdsLengthMeter.Set(milestoneIDLength)

	m.finality.Unlock()
}

// This function will unlock the locked sprint
func (m *milestone) UnlockSprint(endBlockNum uint64) {
	if endBlockNum < m.LockedMilestoneNumber {
		return
	}

	m.Locked = false
	m.purgeMilestoneIDsList()

	err := rawdb.WriteLockField(m.db, m.Locked, m.LockedMilestoneNumber, m.LockedMilestoneHash, m.LockedMilestoneIDs)

	if err != nil {
		log.Error("Error in writing lock data of milestone to db", "err", err)
	}
}

// This function will remove the stored milestoneID
func (m *milestone) RemoveMilestoneID(milestoneId string) {
	m.finality.Lock()
	defer m.finality.Unlock()

	delete(m.LockedMilestoneIDs, milestoneId)

	if len(m.LockedMilestoneIDs) == 0 {
		m.Locked = false
	}

	err := rawdb.WriteLockField(m.db, m.Locked, m.LockedMilestoneNumber, m.LockedMilestoneHash, m.LockedMilestoneIDs)

	if err != nil {
		log.Error("Error in writing lock data of milestone to db", "err", err)
	}

}

// This will check whether the incoming chain matches the locked sprint hash
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

// This will return the list of milestoneIDs stored.
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
	if len(m.FutureMilestoneOrder) < m.MaxCapacity {
		m.enqueueFutureMilestone(num, hash)
	}

	if num < m.LockedMilestoneNumber {
		return
	}

	m.Locked = false
	m.purgeMilestoneIDsList()

	err := rawdb.WriteLockField(m.db, m.Locked, m.LockedMilestoneNumber, m.LockedMilestoneHash, m.LockedMilestoneIDs)

	if err != nil {
		log.Error("Error in writing lock data of milestone to db", "err", err)
	}
}

// EnqueueFutureMilestone add the future milestone to the list
func (m *milestone) enqueueFutureMilestone(key uint64, hash common.Hash) {
	if _, ok := m.FutureMilestoneList[key]; ok {
		log.Debug("Future milestone already exist", "endBlockNumber", key, "futureMilestoneHash", hash)
		return
	}

	log.Debug("Enqueing new future milestone", "endBlockNumber", key, "futureMilestoneHash", hash)

	m.FutureMilestoneList[key] = hash
	m.FutureMilestoneOrder = append(m.FutureMilestoneOrder, key)

	err := rawdb.WriteFutureMilestoneList(m.db, m.FutureMilestoneOrder, m.FutureMilestoneList)
	if err != nil {
		log.Error("Error in writing future milestone data to db", "err", err)
	}

	FutureMilestoneMeter.Set(key)
}

// DequeueFutureMilestone remove the future milestone entry from the list.
func (m *milestone) dequeueFutureMilestone() {
	delete(m.FutureMilestoneList, m.FutureMilestoneOrder[0])
	m.FutureMilestoneOrder = m.FutureMilestoneOrder[1:]

	err := rawdb.WriteFutureMilestoneList(m.db, m.FutureMilestoneOrder, m.FutureMilestoneList)
	if err != nil {
		log.Error("Error in writing future milestone data to db", "err", err)
	}
}
