// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package clique

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type Vote struct {
	Signer    common.Address `json:"signer"`    // Authorized signer that cast this vote
	Block     uint64         `json:"block"`     // Block number the vote was cast in (expire old votes)
	Address   common.Address `json:"address"`   // Account being voted on to change its authorization
	Authorize bool           `json:"authorize"` // Whether to authorize or deauthorize the voted account
}

type Tally struct {
	Authorize bool `json:"authorize"` // Whether the vote is about authorizing or kicking someone
	Votes     int  `json:"votes"`     // Number of votes until now wanting to pass the proposal
}

type Snapshot struct {
	config *chain.CliqueConfig // Rules engine parameters to fine tune behavior

	Number  uint64                        `json:"number"`  // Block number where the snapshot was created
	Hash    common.Hash                   `json:"hash"`    // Block hash where the snapshot was created
	Signers map[accounts.Address]struct{} `json:"signers"` // Set of authorized signers at this moment
	Recents map[uint64]accounts.Address   `json:"recents"` // Set of recent signers for spam protections
	Votes   []*Vote                       `json:"votes"`   // List of votes cast in chronological order
	Tally   map[accounts.Address]Tally    `json:"tally"`   // Current vote tally to avoid recalculating
}

type SignersAscending = common.Addresses

func newSnapshot(config *chain.CliqueConfig, number uint64, hash common.Hash, signers []common.Address) *Snapshot {
	snap := &Snapshot{
		config:  config,
		Number:  number,
		Hash:    hash,
		Signers: make(map[accounts.Address]struct{}),
		Recents: make(map[uint64]accounts.Address),
		Tally:   make(map[accounts.Address]Tally),
	}

	for _, signer := range signers {
		snap.Signers[accounts.InternAddress(signer)] = struct{}{}
	}

	return snap
}

func loadSnapshot(config *chain.CliqueConfig, db kv.RwDB, num uint64, hash common.Hash) (*Snapshot, error) {
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

func (s *Snapshot) store(db kv.RwDB) error {
	blob, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.CliqueSeparate, SnapshotFullKey(s.Number, s.Hash), blob)
	})
}

func (s *Snapshot) validVote(address accounts.Address, authorize bool) bool {
	_, signer := s.Signers[address]
	return (signer && !authorize) || (!signer && authorize)
}

func (s *Snapshot) cast(address accounts.Address, authorize bool) bool {
	if !s.validVote(address, authorize) {
		return false
	}
	if old, ok := s.Tally[address]; ok {
		old.Votes++
		s.Tally[address] = old
	} else {
		s.Tally[address] = Tally{Authorize: authorize, Votes: 1}
	}
	return true
}

func (s *Snapshot) uncast(address accounts.Address, authorize bool) bool {
	tally, ok := s.Tally[address]
	if !ok {
		return false
	}
	if tally.Authorize != authorize {
		return false
	}
	if tally.Votes > 1 {
		tally.Votes--
		s.Tally[address] = tally
	} else {
		delete(s.Tally, address)
	}
	return true
}

func (s *Snapshot) apply(sigcache *lru.Cache[common.Hash, accounts.Address], logger log.Logger, headers ...*types.Header) (*Snapshot, error) {
	if len(headers) == 0 {
		return s, nil
	}
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			return nil, errInvalidVotingChain
		}
	}
	if headers[0].Number.Uint64() != s.Number+1 {
		return nil, errInvalidVotingChain
	}
	snap := s.copy()

	var (
		start  = time.Now()
		logged = time.Now()
	)
	for i, header := range headers {
		number := header.Number.Uint64()
		if number%s.config.Epoch == 0 {
			snap.Votes = nil
			snap.Tally = make(map[accounts.Address]Tally)
		}
		if limit := uint64(len(snap.Signers)/2 + 1); number >= limit {
			delete(snap.Recents, number-limit)
		}
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

		for i, vote := range snap.Votes {
			if vote.Signer == signer.Value() && vote.Address == header.Coinbase {
				snap.uncast(accounts.InternAddress(vote.Address), vote.Authorize)

				snap.Votes = append(snap.Votes[:i], snap.Votes[i+1:]...)
				break // only one vote allowed
			}
		}
		var authorize bool
		switch {
		case bytes.Equal(header.Nonce[:], NonceAuthVote):
			authorize = true
		case bytes.Equal(header.Nonce[:], nonceDropVote):
			authorize = false
		default:
			return nil, errInvalidVote
		}
		coinbase := accounts.InternAddress(header.Coinbase)
		if snap.cast(coinbase, authorize) {
			snap.Votes = append(snap.Votes, &Vote{
				Signer:    signer.Value(),
				Block:     number,
				Address:   header.Coinbase,
				Authorize: authorize,
			})
		}
		if tally := snap.Tally[coinbase]; tally.Votes > len(snap.Signers)/2 {
			if tally.Authorize {
				snap.Signers[coinbase] = struct{}{}
			} else {
				delete(snap.Signers, coinbase)

				if limit := uint64(len(snap.Signers)/2 + 1); number >= limit {
					delete(snap.Recents, number-limit)
				}
				for i := 0; i < len(snap.Votes); i++ {
					if snap.Votes[i].Signer == header.Coinbase {
						snap.uncast(accounts.InternAddress(snap.Votes[i].Address), snap.Votes[i].Authorize)

						snap.Votes = append(snap.Votes[:i], snap.Votes[i+1:]...)

						i--
					}
				}
			}
			for i := 0; i < len(snap.Votes); i++ {
				if snap.Votes[i].Address == header.Coinbase {
					snap.Votes = append(snap.Votes[:i], snap.Votes[i+1:]...)
					i--
				}
			}
			delete(snap.Tally, coinbase)
		}
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

func (s *Snapshot) copy() *Snapshot {
	return &Snapshot{
		config:  s.config,
		Number:  s.Number,
		Hash:    s.Hash,
		Signers: maps.Clone(s.Signers),
		Recents: maps.Clone(s.Recents),
		Votes:   slices.Clone(s.Votes),
		Tally:   maps.Clone(s.Tally),
	}
}

func (s *Snapshot) GetSigners() []common.Address {
	sigs := make(common.Addresses, 0, len(s.Signers))
	for sig := range s.Signers {
		sigs = append(sigs, sig.Value())
	}
	sigs.Sort()
	return sigs
}

func (s *Snapshot) inturn(number uint64, signer common.Address) bool {
	signers, offset := s.GetSigners(), 0
	for offset < len(signers) && signers[offset] != signer {
		offset++
	}
	return (number % uint64(len(signers))) == uint64(offset)
}
