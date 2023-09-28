package bor

import (
	"bytes"
	"context"
	"encoding/json"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

// Snapshot is the state of the authorization voting at a given point in time.
type Snapshot struct {
	config   *chain.BorConfig                           // Consensus engine parameters to fine tune behavior
	sigcache *lru.ARCCache[common.Hash, common.Address] // Cache of recent block signatures to speed up ecrecover

	Number       uint64                    `json:"number"`       // Block number where the snapshot was created
	Hash         common.Hash               `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *valset.ValidatorSet      `json:"validatorSet"` // Validator set at this moment
	Recents      map[uint64]common.Address `json:"recents"`      // Set of recent signers for spam protections
}

const BorSeparate = "BorSeparate"

// signersAscending implements the sort interface to allow sorting a list of addresses
// type signersAscending []common.Address

// func (s signersAscending) Len() int           { return len(s) }
// func (s signersAscending) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }
// func (s signersAscending) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// newSnapshot creates a new snapshot with the specified startup parameters. This
// method does not initialize the set of recent signers, so only ever use if for
// the genesis block.
func newSnapshot(
	config *chain.BorConfig,
	sigcache *lru.ARCCache[common.Hash, common.Address],
	number uint64,
	hash common.Hash,
	validators []*valset.Validator,
	logger log.Logger,
) *Snapshot {
	snap := &Snapshot{
		config:       config,
		sigcache:     sigcache,
		Number:       number,
		Hash:         hash,
		ValidatorSet: valset.NewValidatorSet(validators, logger),
		Recents:      make(map[uint64]common.Address),
	}
	return snap
}

// loadSnapshot loads an existing snapshot from the database.
func loadSnapshot(config *chain.BorConfig, sigcache *lru.ARCCache[common.Hash, common.Address], db kv.RwDB, hash common.Hash) (*Snapshot, error) {
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	blob, err := tx.GetOne(kv.BorSeparate, append([]byte("bor-"), hash[:]...))
	if err != nil {
		return nil, err
	}

	snap := new(Snapshot)

	if err := json.Unmarshal(blob, snap); err != nil {
		return nil, err
	}

	snap.ValidatorSet.UpdateValidatorMap()

	snap.config = config
	snap.sigcache = sigcache

	// update total voting power
	if err := snap.ValidatorSet.UpdateTotalVotingPower(); err != nil {
		return nil, err
	}

	return snap, nil
}

// store inserts the snapshot into the database.
func (s *Snapshot) store(db kv.RwDB) error {
	blob, err := json.Marshal(s)
	if err != nil {
		return err
	}

	return db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.BorSeparate, append([]byte("bor-"), s.Hash[:]...), blob)
	})
}

// copy creates a deep copy of the snapshot, though not the individual votes.
func (s *Snapshot) copy() *Snapshot {
	cpy := &Snapshot{
		config:       s.config,
		sigcache:     s.sigcache,
		Number:       s.Number,
		Hash:         s.Hash,
		ValidatorSet: s.ValidatorSet.Copy(),
		Recents:      make(map[uint64]common.Address),
	}
	for block, signer := range s.Recents {
		cpy.Recents[block] = signer
	}

	return cpy
}

func (s *Snapshot) apply(headers []*types.Header, logger log.Logger) (*Snapshot, error) {
	// Allow passing in no headers for cleaner code
	if len(headers) == 0 {
		return s, nil
	}
	// Sanity check that the headers can be applied
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			return nil, errOutOfRangeChain
		}
	}

	if headers[0].Number.Uint64() != s.Number+1 {
		return nil, errOutOfRangeChain
	}
	// Iterate through the headers and create a new snapshot
	snap := s.copy()

	for _, header := range headers {
		// Remove any votes on checkpoint blocks
		number := header.Number.Uint64()
		sprintLen := s.config.CalculateSprint(number)

		// Delete the oldest signer from the recent list to allow it signing again
		if number >= sprintLen {
			delete(snap.Recents, number-sprintLen)
		}
		// Resolve the authorization key and check against signers
		signer, err := ecrecover(header, s.sigcache, s.config)

		if err != nil {
			return nil, err
		}

		var validSigner bool

		// check if signer is in validator set
		if snap.ValidatorSet.HasAddress(signer) {
			if _, err = snap.GetSignerSuccessionNumber(signer); err != nil {
				return nil, err
			}

			// add recents
			snap.Recents[number] = signer

			validSigner = true
		}

		// change validator set and change proposer
		if number > 0 && (number+1)%sprintLen == 0 {
			if err := validateHeaderExtraField(header.Extra); err != nil {
				return nil, err
			}
			validatorBytes := header.Extra[extraVanity : len(header.Extra)-extraSeal]

			// get validators from headers and use that for new validator set
			newVals, _ := valset.ParseValidators(validatorBytes)
			v := getUpdatedValidatorSet(snap.ValidatorSet.Copy(), newVals, logger)
			v.IncrementProposerPriority(1, logger)
			snap.ValidatorSet = v
		}

		if number > 64 && !validSigner {
			return nil, &UnauthorizedSignerError{number, signer.Bytes()}
		}
	}

	snap.Number += uint64(len(headers))
	snap.Hash = headers[len(headers)-1].Hash()

	return snap, nil
}

// GetSignerSuccessionNumber returns the relative position of signer in terms of the in-turn proposer
func (s *Snapshot) GetSignerSuccessionNumber(signer common.Address) (int, error) {
	validators := s.ValidatorSet.Validators
	proposer := s.ValidatorSet.GetProposer().Address
	proposerIndex, _ := s.ValidatorSet.GetByAddress(proposer)

	if proposerIndex == -1 {
		return -1, &UnauthorizedProposerError{s.Number, proposer.Bytes()}
	}

	signerIndex, _ := s.ValidatorSet.GetByAddress(signer)

	if signerIndex == -1 {
		return -1, &UnauthorizedSignerError{s.Number, signer.Bytes()}
	}

	tempIndex := signerIndex
	if proposerIndex != tempIndex {
		if tempIndex < proposerIndex {
			tempIndex = tempIndex + len(validators)
		}
	}

	return tempIndex - proposerIndex, nil
}

// signers retrieves the list of authorized signers in ascending order.
func (s *Snapshot) signers() []common.Address {
	sigs := make([]common.Address, 0, len(s.ValidatorSet.Validators))
	for _, sig := range s.ValidatorSet.Validators {
		sigs = append(sigs, sig.Address)
	}

	return sigs
}

// Difficulty returns the difficulty for a particular signer at the current snapshot number
func (s *Snapshot) Difficulty(signer common.Address) uint64 {
	// if signer is empty
	if bytes.Equal(signer.Bytes(), common.Address{}.Bytes()) {
		return 1
	}

	validators := s.ValidatorSet.Validators
	proposer := s.ValidatorSet.GetProposer().Address
	totalValidators := len(validators)

	proposerIndex, _ := s.ValidatorSet.GetByAddress(proposer)
	signerIndex, _ := s.ValidatorSet.GetByAddress(signer)

	// temp index
	tempIndex := signerIndex
	if tempIndex < proposerIndex {
		tempIndex = tempIndex + totalValidators
	}

	return uint64(totalValidators - (tempIndex - proposerIndex))
}
