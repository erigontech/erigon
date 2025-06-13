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
	"context"
	"encoding/binary"
	"encoding/json"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/erigontech/erigon-lib/fastjson"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
)

// Snapshot is the state of the authorization voting at a given point in time.
type Snapshot struct {
	config   *borcfg.BorConfig                          // Consensus engine parameters to fine tune behavior
	sigcache *lru.ARCCache[common.Hash, common.Address] // Cache of recent block signatures to speed up ecrecover

	Number       uint64               `json:"number"`       // Block number where the snapshot was created
	Hash         common.Hash          `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *valset.ValidatorSet `json:"validatorSet"` // Validator set at this moment
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
func NewSnapshot(
	config *borcfg.BorConfig,
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
		ValidatorSet: valset.NewValidatorSet(validators),
	}
	return snap
}

// loadSnapshot loads an existing snapshot from the database.
func LoadSnapshot(config *borcfg.BorConfig, sigcache *lru.ARCCache[common.Hash, common.Address], db kv.RwDB, hash common.Hash) (*Snapshot, error) {
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
func (s *Snapshot) Store(db kv.RwDB) error {
	blob, err := fastjson.Marshal(s)
	if err != nil {
		return err
	}

	return db.Update(context.Background(), func(tx kv.RwTx) error {
		err := tx.Put(kv.BorSeparate, append([]byte("bor-"), s.Hash[:]...), blob)
		if err != nil {
			return err
		}
		progressBytes, err := tx.GetOne(kv.BorSeparate, []byte("bor-snapshot-progress"))
		if err != nil {
			return err
		}

		var progress uint64

		if len(progressBytes) == 8 {
			progress = binary.BigEndian.Uint64(progressBytes)
		}

		if s.Number > progress {
			updateBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(updateBytes, s.Number)
			if err = tx.Put(kv.BorSeparate, []byte("bor-snapshot-progress"), updateBytes); err != nil {
				return err
			}
		}
		return nil
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
	}
	return cpy
}

func (s *Snapshot) Apply(parent *types.Header, headers []*types.Header, logger log.Logger) (*Snapshot, error) {
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

		if err := ValidateHeaderTime(header, time.Now(), parent, snap.ValidatorSet, s.config, s.sigcache); err != nil {
			return snap, err
		}

		signer, err := Ecrecover(header, s.sigcache, s.config)
		if err != nil {
			return nil, err
		}

		difficulty := snap.ValidatorSet.SafeDifficulty(signer)
		if header.Difficulty.Uint64() != difficulty {
			return snap, &WrongDifficultyError{number, difficulty, header.Difficulty.Uint64(), signer.Bytes()}
		}

		// change validator set and change proposer
		if number > 0 && s.config.IsSprintEnd(number) {
			if err := ValidateHeaderExtraLength(header.Extra); err != nil {
				return snap, err
			}
			validatorBytes := GetValidatorBytes(header, s.config)

			// get validators from headers and use that for new validator set
			newVals, _ := valset.ParseValidators(validatorBytes)
			v := valset.GetUpdatedValidatorSet(snap.ValidatorSet.Copy(), newVals, logger)
			v.IncrementProposerPriority(1)
			snap.ValidatorSet = v
		}

		parent = header
		snap.Number = number
		snap.Hash = header.Hash()
	}

	return snap, nil
}

func (s *Snapshot) GetSignerSuccessionNumber(signer common.Address) (int, error) {
	return s.ValidatorSet.GetSignerSuccessionNumber(signer, s.Number)
}

// signers retrieves the list of authorized signers in ascending order.
func (s *Snapshot) signers() []common.Address {
	sigs := make([]common.Address, 0, len(s.ValidatorSet.Validators))
	for _, sig := range s.ValidatorSet.Validators {
		sigs = append(sigs, sig.Address)
	}

	return sigs
}
