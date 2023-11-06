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

package clique

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"sort"
	"time"

	"github.com/goccy/go-json"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

// Vote represents a single vote that an authorized signer made to modify the
// list of authorizations.
type Vote struct {
	Signer    libcommon.Address `json:"signer"`    // Authorized signer that cast this vote
	Block     uint64            `json:"block"`     // Block number the vote was cast in (expire old votes)
	Address   libcommon.Address `json:"address"`   // Account being voted on to change its authorization
	Authorize bool              `json:"authorize"` // Whether to authorize or deauthorize the voted account
}

// Tally is a simple vote tally to keep the current score of votes. Votes that
// go against the proposal aren't counted since it's equivalent to not voting.
type Tally struct {
	Authorize bool `json:"authorize"` // Whether the vote is about authorizing or kicking someone
	Votes     int  `json:"votes"`     // Number of votes until now wanting to pass the proposal
}

// Snapshot is the state of the authorization voting at a given point in time.
type Snapshot struct {
	config *chain.CliqueConfig // Consensus engine parameters to fine tune behavior

	Number  uint64                         `json:"number"`  // Block number where the snapshot was created
	Hash    libcommon.Hash                 `json:"hash"`    // Block hash where the snapshot was created
	Signers map[libcommon.Address]struct{} `json:"signers"` // Set of authorized signers at this moment
	Recents map[uint64]libcommon.Address   `json:"recents"` // Set of recent signers for spam protections
	Votes   []*Vote                        `json:"votes"`   // List of votes cast in chronological order
	Tally   map[libcommon.Address]Tally    `json:"tally"`   // Current vote tally to avoid recalculating
}

// signersAscending implements the sort interface to allow sorting a list of addresses
type SignersAscending []libcommon.Address

func (s SignersAscending) Len() int           { return len(s) }
func (s SignersAscending) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }
func (s SignersAscending) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// newSnapshot creates a new snapshot with the specified startup parameters. This
// method does not initialize the set of recent signers, so only ever use if for
// the genesis block.
func newSnapshot(config *chain.CliqueConfig, number uint64, hash libcommon.Hash, signers []libcommon.Address) *Snapshot {
	snap := &Snapshot{
		config:  config,
		Number:  number,
		Hash:    hash,
		Signers: make(map[libcommon.Address]struct{}),
		Recents: make(map[uint64]libcommon.Address),
		Tally:   make(map[libcommon.Address]Tally),
	}

	for _, signer := range signers {
		snap.Signers[signer] = struct{}{}
	}

	return snap
}

// loadSnapshot loads an existing snapshot from the database.
func loadSnapshot(config *chain.CliqueConfig, db kv.RwDB, num uint64, hash libcommon.Hash) (*Snapshot, error) {
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blob, err := tx.GetOne(kv.CliqueSeparate, SnapshotFullKey(num, hash))
	if err != nil {
		return nil, err
	}

	snap := new(Snapshot)
	if err := json.Unmarshal(blob, snap); err != nil {
		return nil, err
	}
	snap.config = config

	return snap, nil
}

var ErrNotFound = errors.New("not found")

func lastSnapshot(db kv.RwDB, logger log.Logger) (uint64, error) {
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	lastEnc, err := tx.GetOne(kv.CliqueLastSnapshot, LastSnapshotKey())
	if err != nil {
		return 0, fmt.Errorf("failed check last clique snapshot: %w", err)
	}
	if len(lastEnc) == 0 {
		return 0, ErrNotFound
	}

	lastNum, err := dbutils.DecodeBlockNumber(lastEnc)
	if err != nil {
		logger.Error("can't decode last snapshot", "err", err)
		return 0, ErrNotFound
	}

	return lastNum, nil
}

// store inserts the snapshot into the database.
func (s *Snapshot) store(db kv.RwDB) error {
	blob, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.CliqueSeparate, SnapshotFullKey(s.Number, s.Hash), blob)
	})
}

// validVote returns whether it makes sense to cast the specified vote in the
// given snapshot context (e.g. don't try to add an already authorized signer).
func (s *Snapshot) validVote(address libcommon.Address, authorize bool) bool {
	_, signer := s.Signers[address]
	return (signer && !authorize) || (!signer && authorize)
}

// cast adds a new vote into the tally.
func (s *Snapshot) cast(address libcommon.Address, authorize bool) bool {
	// Ensure the vote is meaningful
	if !s.validVote(address, authorize) {
		return false
	}
	// Cast the vote into an existing or new tally
	if old, ok := s.Tally[address]; ok {
		old.Votes++
		s.Tally[address] = old
	} else {
		s.Tally[address] = Tally{Authorize: authorize, Votes: 1}
	}
	return true
}

// uncast removes a previously cast vote from the tally.
func (s *Snapshot) uncast(address libcommon.Address, authorize bool) bool {
	// If there's no tally, it's a dangling vote, just drop
	tally, ok := s.Tally[address]
	if !ok {
		return false
	}
	// Ensure we only revert counted votes
	if tally.Authorize != authorize {
		return false
	}
	// Otherwise revert the vote
	if tally.Votes > 1 {
		tally.Votes--
		s.Tally[address] = tally
	} else {
		delete(s.Tally, address)
	}
	return true
}

// apply creates a new authorization snapshot by applying the given headers to
// the original one.
func (s *Snapshot) apply(sigcache *lru.ARCCache[libcommon.Hash, libcommon.Address], logger log.Logger, headers ...*types.Header) (*Snapshot, error) {
	// Allow passing in no headers for cleaner code
	if len(headers) == 0 {
		return s, nil
	}
	// Sanity check that the headers can be applied
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			return nil, errInvalidVotingChain
		}
	}
	if headers[0].Number.Uint64() != s.Number+1 {
		return nil, errInvalidVotingChain
	}
	// Iterate through the headers and create a new snapshot
	snap := s.copy()

	var (
		start  = time.Now()
		logged = time.Now()
	)
	for i, header := range headers {
		// Remove any votes on checkpoint blocks
		number := header.Number.Uint64()
		if number%s.config.Epoch == 0 {
			snap.Votes = nil
			snap.Tally = make(map[libcommon.Address]Tally)
		}
		// Delete the oldest signer from the recent list to allow it signing again
		if limit := uint64(len(snap.Signers)/2 + 1); number >= limit {
			delete(snap.Recents, number-limit)
		}
		// Resolve the authorization key and check against signers
		signer, err := ecrecover(header, sigcache)
		if err != nil {
			return nil, err
		}
		if _, ok := snap.Signers[signer]; !ok {
			return nil, ErrUnauthorizedSigner
		}
		for _, recent := range snap.Recents {
			if recent == signer {
				return nil, ErrRecentlySigned
			}
		}
		snap.Recents[number] = signer

		// Header authorized, discard any previous votes from the signer
		for i, vote := range snap.Votes {
			if vote.Signer == signer && vote.Address == header.Coinbase {
				// Uncast the vote from the cached tally
				snap.uncast(vote.Address, vote.Authorize)

				// Uncast the vote from the chronological list
				snap.Votes = append(snap.Votes[:i], snap.Votes[i+1:]...)
				break // only one vote allowed
			}
		}
		// Tally up the new vote from the signer
		var authorize bool
		switch {
		case bytes.Equal(header.Nonce[:], NonceAuthVote):
			authorize = true
		case bytes.Equal(header.Nonce[:], nonceDropVote):
			authorize = false
		default:
			return nil, errInvalidVote
		}
		if snap.cast(header.Coinbase, authorize) {
			snap.Votes = append(snap.Votes, &Vote{
				Signer:    signer,
				Block:     number,
				Address:   header.Coinbase,
				Authorize: authorize,
			})
		}
		// If the vote passed, update the list of signers
		if tally := snap.Tally[header.Coinbase]; tally.Votes > len(snap.Signers)/2 {
			if tally.Authorize {
				snap.Signers[header.Coinbase] = struct{}{}
			} else {
				delete(snap.Signers, header.Coinbase)

				// Signer list shrunk, delete any leftover recent caches
				if limit := uint64(len(snap.Signers)/2 + 1); number >= limit {
					delete(snap.Recents, number-limit)
				}
				// Discard any previous votes the deauthorized signer cast
				for i := 0; i < len(snap.Votes); i++ {
					if snap.Votes[i].Signer == header.Coinbase {
						// Uncast the vote from the cached tally
						snap.uncast(snap.Votes[i].Address, snap.Votes[i].Authorize)

						// Uncast the vote from the chronological list
						snap.Votes = append(snap.Votes[:i], snap.Votes[i+1:]...)

						i--
					}
				}
			}
			// Discard any previous votes around the just changed account
			for i := 0; i < len(snap.Votes); i++ {
				if snap.Votes[i].Address == header.Coinbase {
					snap.Votes = append(snap.Votes[:i], snap.Votes[i+1:]...)
					i--
				}
			}
			delete(snap.Tally, header.Coinbase)
		}
		// If we're taking too much time (ecrecover), notify the user once a while
		if time.Since(logged) > 8*time.Second {
			logger.Info("Reconstructing voting history", "processed", i, "total", len(headers), "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}
	if time.Since(start) > 8*time.Second {
		logger.Info("Reconstructed voting history", "processed", len(headers), "elapsed", common.PrettyDuration(time.Since(start)))
	}
	snap.Number += uint64(len(headers))
	snap.Hash = headers[len(headers)-1].Hash()

	return snap, nil
}

// copy creates a deep copy of the snapshot, though not the individual votes.
func (s *Snapshot) copy() *Snapshot {
	cpy := &Snapshot{
		config:  s.config,
		Number:  s.Number,
		Hash:    s.Hash,
		Signers: make(map[libcommon.Address]struct{}),
		Recents: make(map[uint64]libcommon.Address),
		Votes:   make([]*Vote, len(s.Votes)),
		Tally:   make(map[libcommon.Address]Tally),
	}
	for signer := range s.Signers {
		cpy.Signers[signer] = struct{}{}
	}
	for block, signer := range s.Recents {
		cpy.Recents[block] = signer
	}
	for address, tally := range s.Tally {
		cpy.Tally[address] = tally
	}
	copy(cpy.Votes, s.Votes)

	return cpy
}

// signers retrieves the list of authorized signers in ascending order.
func (s *Snapshot) GetSigners() []libcommon.Address {
	sigs := make([]libcommon.Address, 0, len(s.Signers))
	for sig := range s.Signers {
		sigs = append(sigs, sig)
	}
	sort.Sort(SignersAscending(sigs))
	return sigs
}

// inturn returns if a signer at a given block height is in-turn or not.
func (s *Snapshot) inturn(number uint64, signer libcommon.Address) bool {
	signers, offset := s.GetSigners(), 0
	for offset < len(signers) && signers[offset] != signer {
		offset++
	}
	return (number % uint64(len(signers))) == uint64(offset)
}
