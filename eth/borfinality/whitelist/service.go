package whitelist

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
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

// IsValidPeer checks if the chain we're about to receive from a peer is valid or not
// in terms of reorgs. We won't reorg beyond the last bor checkpoint submitted to mainchain and last milestone voted in the heimdall
func (s *Service) IsValidPeer(fetchHeadersByNumber func(number uint64, amount int, skip int, reverse bool) ([]*types.Header, []common.Hash, error)) (bool, error) {
	checkpointBool, err := s.checkpointService.IsValidPeer(fetchHeadersByNumber)
	if !checkpointBool {
		return checkpointBool, err
	}

	milestoneBool, err := s.milestoneService.IsValidPeer(fetchHeadersByNumber)
	if !milestoneBool {
		return milestoneBool, err
	}

	return true, nil
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

// FIXME: remoteHeader is not used
func isValidPeer(fetchHeadersByNumber func(number uint64, amount int, skip int, reverse bool) ([]*types.Header, []common.Hash, error), doExist bool, number uint64, hash common.Hash) (bool, error) {
	// Check for availaibility of the last milestone block.
	// This can be also be empty if our heimdall is not responding
	// or we're running without it.
	if !doExist {
		// worst case, we don't have the milestone in memory
		return true, nil
	}

	headers, hashes, err := fetchHeadersByNumber(number, 1, 0, false)
	if err != nil {
		return false, fmt.Errorf("%w: last whitelisted block number %d, err %v", ErrNoRemote, number, err)
	}

	if len(headers) == 0 {
		return false, fmt.Errorf("%w: last whitlisted block number %d", ErrNoRemote, number)
	}

	reqBlockNum := headers[0].Number.Uint64()
	reqBlockHash := hashes[0]

	// Check against the whitelisted blocks
	if reqBlockNum == number && reqBlockHash == hash {
		return true, nil
	}

	return false, ErrMismatch
}
