// Copyright 2017 The go-ethereum Authors
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

package parlia

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
)

// Snapshot is the state of the validatorSet at a given point.
type Snapshot struct {
	config   *chain.ParliaConfig                        // Consensus engine parameters to fine tune behavior
	sigCache *lru.ARCCache[common.Hash, common.Address] // Cache of recent block signatures to speed up ecrecover

	Number           uint64                               `json:"number"`                // Block number where the snapshot was created
	Hash             libcommon.Hash                       `json:"hash"`                  // Block hash where the snapshot was created
	Validators       map[libcommon.Address]*ValidatorInfo `json:"validators"`            // Set of authorized validators at this moment
	Recents          map[uint64]libcommon.Address         `json:"recents"`               // Set of recent validators for spam protections
	RecentForkHashes map[uint64]string                    `json:"recent_fork_hashes"`    // Set of recent forkHash
	Attestation      *types.VoteData                      `json:"attestation:omitempty"` // Attestation for fast finality
}

type ValidatorInfo struct {
	Index       int                `json:"index:omitempty"` // The index should offset by 1
	VoteAddress types.BLSPublicKey `json:"vote_address,omitempty"`
}

// newSnapshot creates a new snapshot with the specified startup parameters. This
// method does not initialize the set of recent validators, so only ever use it for
// the genesis block.
func newSnapshot(
	config *chain.ParliaConfig,
	sigCache *lru.ARCCache[common.Hash, common.Address],
	number uint64,
	hash libcommon.Hash,
	validators []libcommon.Address,
	voteAddrs []types.BLSPublicKey,
) *Snapshot {
	snap := &Snapshot{
		config:           config,
		sigCache:         sigCache,
		Number:           number,
		Hash:             hash,
		Recents:          make(map[uint64]common.Address),
		RecentForkHashes: make(map[uint64]string),
		Validators:       make(map[libcommon.Address]*ValidatorInfo),
	}

	for idx, v := range validators {
		// The luban fork from the genesis block
		if len(voteAddrs) == len(validators) {
			snap.Validators[v] = &ValidatorInfo{
				VoteAddress: voteAddrs[idx],
			}
		} else {
			snap.Validators[v] = &ValidatorInfo{}
		}
	}

	// The luban fork from the genesis block
	if len(voteAddrs) == len(validators) {
		validators := snap.validators()
		for idx, v := range validators {
			snap.Validators[v].Index = idx + 1 // offset by 1
		}
	}
	return snap
}

// validatorsAscending implements the sort interface to allow sorting a list of addresses
type validatorsAscending []common.Address

func (s validatorsAscending) Len() int           { return len(s) }
func (s validatorsAscending) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }
func (s validatorsAscending) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// SnapshotFullKey = SnapshotBucket + num (uint64 big endian) + hash
func SnapshotFullKey(number uint64, hash common.Hash) []byte {
	return append(hexutility.EncodeTs(number), hash.Bytes()...)
}

var ErrNoSnapsnot = fmt.Errorf("no parlia snapshot")

// loadSnapshot loads an existing snapshot from the database.
func loadSnapshot(config *chain.ParliaConfig, sigCache *lru.ARCCache[common.Hash, common.Address], db kv.RwDB, num uint64, hash common.Hash) (*Snapshot, error) {
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blob, err := tx.GetOne(kv.ParliaSnapshot, SnapshotFullKey(num, hash))
	if err != nil {
		return nil, err
	}

	if len(blob) == 0 {
		return nil, ErrNoSnapsnot
	}
	snap := new(Snapshot)
	if err := json.Unmarshal(blob, snap); err != nil {
		return nil, err
	}
	snap.config = config
	snap.sigCache = sigCache
	return snap, nil
}

// store inserts the snapshot into the database.
func (s *Snapshot) store(db kv.RwDB) error {
	blob, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return db.UpdateNosync(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.ParliaSnapshot, SnapshotFullKey(s.Number, s.Hash), blob)
	})
}

// copy creates a deep copy of the snapshot
func (s *Snapshot) copy() *Snapshot {
	cpy := &Snapshot{
		config:           s.config,
		sigCache:         s.sigCache,
		Number:           s.Number,
		Hash:             s.Hash,
		Validators:       make(map[libcommon.Address]*ValidatorInfo),
		Recents:          make(map[uint64]libcommon.Address),
		RecentForkHashes: make(map[uint64]string),
	}

	for v := range s.Validators {
		cpy.Validators[v] = &ValidatorInfo{
			Index:       s.Validators[v].Index,
			VoteAddress: s.Validators[v].VoteAddress,
		}
	}
	for block, v := range s.Recents {
		cpy.Recents[block] = v
	}
	for block, id := range s.RecentForkHashes {
		cpy.RecentForkHashes[block] = id
	}
	if s.Attestation != nil {
		cpy.Attestation = &types.VoteData{
			SourceNumber: s.Attestation.SourceNumber,
			SourceHash:   s.Attestation.SourceHash,
			TargetNumber: s.Attestation.TargetNumber,
			TargetHash:   s.Attestation.TargetHash,
		}
	}
	return cpy
}

// nolint
func (s *Snapshot) isMajorityFork(forkHash string) bool {
	ally := 0
	for _, h := range s.RecentForkHashes {
		if h == forkHash {
			ally++
		}
	}
	return ally > len(s.RecentForkHashes)/2
}

func (s *Snapshot) updateAttestation(header *types.Header, chainConfig *chain.Config, parliaConfig *chain.ParliaConfig) {
	// The attestation should have been checked in verify header, update directly
	attestation, _ := getVoteAttestationFromHeader(header, chainConfig, parliaConfig)
	if attestation == nil {
		return
	}

	// Update attestation
	s.Attestation = &types.VoteData{
		SourceNumber: attestation.Data.SourceNumber,
		SourceHash:   attestation.Data.SourceHash,
		TargetNumber: attestation.Data.TargetNumber,
		TargetHash:   attestation.Data.TargetHash,
	}
}

func (s *Snapshot) apply(headers []*types.Header, chain consensus.ChainHeaderReader, parents []*types.Header, chainConfig *chain.Config, verifiedAttestations map[libcommon.Hash]struct{}, doLog bool) (*Snapshot, error) {
	// Allow passing in no headers for cleaner code
	if len(headers) == 0 {
		return s, nil
	}
	// Sanity check that the headers can be applied
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			return nil, errOutOfRangeChain
		}
		if !bytes.Equal(headers[i+1].ParentHash.Bytes(), headers[i].Hash().Bytes()) {
			return nil, errBlockHashInconsistent
		}
	}
	if headers[0].Number.Uint64() != s.Number+1 {
		return nil, errOutOfRangeChain
	}
	if !bytes.Equal(headers[0].ParentHash.Bytes(), s.Hash.Bytes()) {
		return nil, errBlockHashInconsistent
	}
	// Iterate through the headers and create a new snapshot
	snap := s.copy()

	for _, header := range headers {
		number := header.Number.Uint64()
		if doLog && number%100_000 == 0 {
			log.Info("[parlia] snapshots build, recover from headers", "block", number)
		}
		// Delete the oldest validator from the recent list to allow it signing again
		if limit := uint64(len(snap.Validators)/2 + 1); number >= limit {
			delete(snap.Recents, number-limit)
		}
		if limit := uint64(len(snap.Validators)); number >= limit {
			delete(snap.RecentForkHashes, number-limit)
		}
		// Resolve the authorization key and check against signers
		validator, err := ecrecover(header, s.sigCache, chainConfig.ChainID)
		if err != nil {
			return nil, err
		}
		// Disabling this validation due to issues with BEP-131 looks like is breaks the property that used to allow Erigon to sync Parlia without knowning the contract state
		// Further investigation is required
		/*
			if _, ok := snap.Validators[validator]; !ok {
				return nil, errUnauthorizedValidator
			}
		*/
		for _, recent := range snap.Recents {
			if recent == validator {
				return nil, errRecentlySigned
			}
		}
		snap.Recents[number] = validator
		// change validator set
		if number > 0 && number%s.config.Epoch == uint64(len(snap.Validators)/2) {
			checkpointHeader := FindAncientHeader(header, uint64(len(snap.Validators)/2), chain, parents)
			if checkpointHeader == nil {
				return nil, consensus.ErrUnknownAncestor
			}

			// get validators from headers and use that for new validator set
			newValArr, voteAddrs, err := parseValidators(checkpointHeader, chainConfig, s.config)
			if err != nil {
				return nil, err
			}

			newVals := make(map[libcommon.Address]*ValidatorInfo, len(newValArr))
			for idx, val := range newValArr {
				if !chainConfig.IsLuban(header.Number.Uint64()) {
					newVals[val] = &ValidatorInfo{}
				} else {
					newVals[val] = &ValidatorInfo{
						VoteAddress: voteAddrs[idx],
					}
				}
			}
			oldLimit := len(snap.Validators)/2 + 1
			newLimit := len(newVals)/2 + 1
			if newLimit < oldLimit {
				for i := 0; i < oldLimit-newLimit; i++ {
					delete(snap.Recents, number-uint64(newLimit)-uint64(i))
				}
			}
			oldLimit = len(snap.Validators)
			newLimit = len(newVals)
			if newLimit < oldLimit {
				for i := 0; i < oldLimit-newLimit; i++ {
					delete(snap.RecentForkHashes, number-uint64(newLimit)-uint64(i))
				}
			}
			snap.Validators = newVals
			if chainConfig.IsLuban(header.Number.Uint64()) {
				validators := snap.validators()
				for idx, val := range validators {
					snap.Validators[val].Index = idx + 1 // offset by 1
				}
			}
		}

		_, voteAssestationNoErr := verifiedAttestations[header.Hash()]
		if chainConfig.IsPlato(header.Number.Uint64()) || (chainConfig.IsLuban(header.Number.Uint64()) && voteAssestationNoErr) {
			snap.updateAttestation(header, chainConfig, s.config)
		}

		snap.RecentForkHashes[number] = hex.EncodeToString(header.Extra[extraVanity-nextForkHashSize : extraVanity])
	}
	snap.Number += uint64(len(headers))
	snap.Hash = headers[len(headers)-1].Hash()
	return snap, nil
}

// validators retrieves the list of validators in ascending order.
func (s *Snapshot) validators() []common.Address {
	validators := make([]common.Address, 0, len(s.Validators))
	for v := range s.Validators {
		validators = append(validators, v)
	}
	sort.Sort(validatorsAscending(validators))
	return validators
}

// inturn returns if a validator at a given block height is in-turn or not.
func (s *Snapshot) inturn(validator common.Address) bool {
	validators := s.validators()
	offset := (s.Number + 1) % uint64(len(validators))
	return validators[offset] == validator
}

func (s *Snapshot) enoughDistance(validator common.Address, header *types.Header) bool {
	idx := s.indexOfVal(validator)
	if idx < 0 {
		return true
	}
	validatorNum := int64(len(s.validators()))
	if validatorNum == 1 {
		return true
	}
	if validator == header.Coinbase {
		return false
	}
	offset := (int64(s.Number) + 1) % validatorNum
	if int64(idx) >= offset {
		return int64(idx)-offset >= validatorNum-2
	} else {
		return validatorNum+int64(idx)-offset >= validatorNum-2
	}
}

func (s *Snapshot) indexOfVal(validator libcommon.Address) int {
	if validator, ok := s.Validators[validator]; ok && validator.Index > 0 {
		return validator.Index - 1 // Index is offset by 1
	}

	validators := s.validators()
	for idx, val := range validators {
		if val == validator {
			return idx
		}
	}
	return -1
}

func (s *Snapshot) supposeValidator() common.Address {
	validators := s.validators()
	index := (s.Number + 1) % uint64(len(validators))
	return validators[index]
}

func parseValidators(header *types.Header, chainConfig *chain.Config, parliaConfig *chain.ParliaConfig) ([]libcommon.Address, []types.BLSPublicKey, error) {
	validatorsBytes := getValidatorBytesFromHeader(header, chainConfig, parliaConfig)
	if len(validatorsBytes) == 0 {
		return nil, nil, errors.New("invalid validators bytes")
	}

	if !chainConfig.IsLuban(header.Number.Uint64()) {
		n := len(validatorsBytes) / validatorBytesLengthBeforeLuban
		result := make([]libcommon.Address, n)
		for i := 0; i < n; i++ {
			result[i] = libcommon.BytesToAddress(validatorsBytes[i*validatorBytesLengthBeforeLuban : (i+1)*validatorBytesLengthBeforeLuban])
		}
		return result, nil, nil
	}

	n := len(validatorsBytes) / validatorBytesLength
	cnsAddrs := make([]libcommon.Address, n)
	voteAddrs := make([]types.BLSPublicKey, n)
	for i := 0; i < n; i++ {
		cnsAddrs[i] = libcommon.BytesToAddress(validatorsBytes[i*validatorBytesLength : i*validatorBytesLength+length.Addr])
		copy(voteAddrs[i][:], validatorsBytes[i*validatorBytesLength+length.Addr:(i+1)*validatorBytesLength])
	}
	return cnsAddrs, voteAddrs, nil
}

func FindAncientHeader(header *types.Header, ite uint64, chain consensus.ChainHeaderReader, candidateParents []*types.Header) *types.Header {
	ancient := header
	for i := uint64(1); i <= ite; i++ {
		parentHash := ancient.ParentHash
		parentHeight := ancient.Number.Uint64() - 1
		found := false
		if len(candidateParents) > 0 {
			index := sort.Search(len(candidateParents), func(i int) bool {
				return candidateParents[i].Number.Uint64() >= parentHeight
			})
			if index < len(candidateParents) && candidateParents[index].Number.Uint64() == parentHeight &&
				candidateParents[index].Hash() == parentHash {
				ancient = candidateParents[index]
				found = true
			}
		}
		if !found {
			ancient = chain.GetHeader(parentHash, parentHeight)
			found = true
		}
		if ancient == nil || !found {
			return nil
		}
	}
	return ancient
}
