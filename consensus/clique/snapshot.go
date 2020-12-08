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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"

	lru "github.com/hashicorp/golang-lru"
	json "github.com/json-iterator/go"
)

// Vote represents a single vote that an authorized signer made to modify the
// list of authorizations.
type Vote struct {
	Signer    common.Address `json:"signer"`    // Authorized signer that cast this vote
	Block     uint64         `json:"block"`     // Block number the vote was cast in (expire old votes)
	Address   common.Address `json:"address"`   // Account being voted on to change its authorization
	Authorize bool           `json:"authorize"` // Whether to authorize or deauthorize the voted account
}

// Tally is a simple vote tally to keep the current score of votes. Votes that
// go against the proposal aren't counted since it's equivalent to not voting.
type Tally struct {
	Authorize bool `json:"authorize"` // Whether the vote is about authorizing or kicking someone
	Votes     int  `json:"votes"`     // Number of votes until now wanting to pass the proposal
}

// Snapshot is the state of the authorization voting at a given point in time.
type Snapshot struct {
	config   *params.CliqueConfig // Consensus engine parameters to fine tune behavior
	sigcache *lru.ARCCache        // Cache of recent block signatures to speed up ecrecover

	Number  uint64                      `json:"number"`  // Block number where the snapshot was created
	Hash    common.Hash                 `json:"hash"`    // Block hash where the snapshot was created
	Signers map[common.Address]struct{} `json:"signers"` // Set of authorized signers at this moment
	Recents map[uint64]common.Address   `json:"recents"` // Set of recent signers for spam protections
	Votes   []*Vote                     `json:"votes"`   // List of votes cast in chronological order
	Tally   map[common.Address]Tally    `json:"tally"`   // Current vote tally to avoid recalculating

	snapStorage storage `json:"-"`
}

// signersAscending implements the sort interface to allow sorting a list of addresses
type signersAscending []common.Address

func (s signersAscending) Len() int           { return len(s) }
func (s signersAscending) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }
func (s signersAscending) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// newSnapshot creates a new snapshot with the specified startup parameters. This
// method does not initialize the set of recent signers, so only ever use if for
// the genesis block.
func newSnapshot(config *params.CliqueConfig, sigcache *lru.ARCCache, number uint64, hash common.Hash, signers []common.Address) *Snapshot {
	snap := &Snapshot{
		config:   config,
		sigcache: sigcache,
		Number:   number,
		Hash:     hash,
		Signers:  make(map[common.Address]struct{}),
		Recents:  make(map[uint64]common.Address),
		Tally:    make(map[common.Address]Tally),
	}
	for _, signer := range signers {
		snap.Signers[signer] = struct{}{}
	}
	return snap
}

// loadSnapshot loads an existing snapshot from the database.
func loadAndFillSnapshot(db ethdb.Database, num uint64, hash common.Hash, config *params.CliqueConfig, sigcache *lru.ARCCache) (*Snapshot, error) {
	snap, err := loadSnapshot(db, num, hash)
	if err != nil {
		return nil, err
	}

	//fmt.Printf("Snapshot.loadAndFillSnapshot for block %q - snap %d(%s): %d\n", hash.String(), snap.Number, snap.Hash.String(), len(snap.Signers))

	snap.config = config
	snap.sigcache = sigcache

	return snap, nil
}

// loadSnapshot loads an existing snapshot from the database.
func loadSnapshot(db ethdb.Database, num uint64, hash common.Hash) (*Snapshot, error) {
	blob, err := getSnapshotData(db, num, hash)
	if err != nil {
		return nil, err
	}

	snap := new(Snapshot)
	if err := json.Unmarshal(blob, snap); err != nil {
		return nil, err
	}

	//fmt.Printf("Snapshot.loadSnapshot for block %q - snap %d(%s): %d\n", hash.String(), snap.Number, snap.Hash.String(), len(snap.Signers))

	return snap, nil
}

func getSnapshotData(db ethdb.Database, num uint64, hash common.Hash) ([]byte, error) {
	return db.Get(dbutils.CliqueBucket, dbutils.BlockBodyKey(num, hash))
}

func hasSnapshotData(db ethdb.Database, num uint64, hash common.Hash) (bool, error) {
	return db.Has(dbutils.CliqueBucket, dbutils.BlockBodyKey(num, hash))
}

func hasSnapshotByBlock(db ethdb.Database, num uint64) (bool, error) {
	enc := dbutils.EncodeBlockNumber(num)
	ok, err := db.Has(dbutils.HeaderNumberPrefix, enc)
	if err != nil {
		return false, err
	}
	return ok, nil
}

func addSnapshotByBlock(db ethdb.Database, num uint64, hash common.Hash) error {
	enc := dbutils.EncodeBlockNumber(num)
	return db.Put(dbutils.HeaderNumberPrefix, enc, hash[:])
}

// store inserts the snapshot into the database.
func (s *Snapshot) store(db ethdb.Database) error {
	t := time.Now()
	blob, err := json.Marshal(s)
	fmt.Println("+++store-0", time.Since(t))
	t = time.Now()
	if err != nil {
		//fmt.Printf("Snapshot.store ERROR snap %d(%s): %v\n", s.Number, s.Hash.String(), err)
		return err
	}

	//fmt.Printf("Snapshot.store snap %d(%s): %v\n", s.Number, s.Hash.String(), spew.Sdump(s.Signers))

	s.snapStorage.save(db, s.Number, s.Hash, blob)
	fmt.Println("+++store-1", time.Since(t), len(blob))
	return err
}

type storage struct {
	ch chan snapObj
	db ethdb.Database
	sync.Once
}

type snapObj struct {
	number uint64
	hash   common.Hash
	blob   []byte
}

func (st *storage) save(db ethdb.Database, number uint64, hash common.Hash, blob []byte) {
	st.Once.Do(func() {
		if st.db == nil {
			st.db = db
		}

		st.ch = make(chan snapObj, 1024)

		go func() {
			var snap snapObj
			for {
				snap = <-st.ch
				_ = st.db.Put(dbutils.CliqueBucket, dbutils.BlockBodyKey(snap.number, snap.hash), snap.blob)
			}
		}()
	})

	st.ch <- snapObj{number, hash, blob}
}

// copy creates a deep copy of the snapshot, though not the individual votes.
func (s *Snapshot) copy() *Snapshot {
	cpy := &Snapshot{
		config:   s.config,
		sigcache: s.sigcache,
		Number:   s.Number,
		Hash:     s.Hash,
		Signers:  make(map[common.Address]struct{}),
		Recents:  make(map[uint64]common.Address),
		Votes:    make([]*Vote, len(s.Votes)),
		Tally:    make(map[common.Address]Tally),
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

// validVote returns whether it makes sense to cast the specified vote in the
// given snapshot context (e.g. don't try to add an already authorized signer).
func (s *Snapshot) validVote(address common.Address, authorize bool) bool {
	_, signer := s.Signers[address]
	return (signer && !authorize) || (!signer && authorize)
}

// cast adds a new vote into the tally.
func (s *Snapshot) cast(address common.Address, authorize bool) bool {
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
func (s *Snapshot) uncast(address common.Address, authorize bool) bool {
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
func (s *Snapshot) apply(headers []*types.Header) (*Snapshot, error) {
	// Allow passing in no headers for cleaner code
	if len(headers) == 0 {
		return s, nil
	}

	// Sanity check that the headers can be applied
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			//fmt.Println("errInvalidVotingChain1")
			return nil, errInvalidVotingChain
		}
	}

	if headers[0].Number.Uint64() != s.Number+1 {
		//fmt.Println("errInvalidVotingChain2", headers[0].Number.Uint64(), headers[len(headers)-1].Number.Uint64(), s.Number+1)
		return nil, errInvalidVotingChain
	}
	// fixme it looks like we don't need .copy()
	// Iterate through the headers and create a new snapshot
	snap := s.copy()

	var (
		start  = time.Now()
		logged = time.Now()
	)

	/*
		var spanStr string
		spanStr += fmt.Sprintf("!!! snap %d with %d headers:\n", s.Number, len(headers))
		spanStr += fmt.Sprintf("\theader:")
		for i := len(headers) - 1; i >= 0; i-- {
			spanStr += fmt.Sprintf(" %d,", headers[i].Number.Uint64())
		}
		fmt.Println(spanStr)
	*/

	for i := len(headers) - 1; i >= 0; i-- {
		header := headers[i]

		// Remove any votes on checkpoint blocks
		number := header.Number.Uint64()
		if number%s.config.Epoch == 0 {
			snap.Votes = nil
			snap.Tally = make(map[common.Address]Tally)
		}
		// Delete the oldest signer from the recent list to allow it signing again
		if limit := uint64(len(snap.Signers)/2 + 1); number >= limit {
			delete(snap.Recents, number-limit)
		}
		// Resolve the authorization key and check against signers
		signer, err := ecrecover(header, s.sigcache)
		if err != nil {
			return nil, err
		}
		if _, ok := snap.Signers[signer]; !ok {
			//fmt.Printf("errUnauthorizedSigner-3 for block %d\n", number)
			return nil, errUnauthorizedSigner
		}
		for _, recent := range snap.Recents {
			if recent == signer {
				//fmt.Println("recently signed2", number, b, recent.String())
				return nil, errRecentlySigned
			}
		}
		snap.Recents[number] = signer

		// Header authorized, discard any previous votes from the signer
		for voteIdx, vote := range snap.Votes {
			if vote.Signer == signer && vote.Address == header.Coinbase {
				// Uncast the vote from the cached tally
				snap.uncast(vote.Address, vote.Authorize)

				// Uncast the vote from the chronological list
				snap.Votes = append(snap.Votes[:voteIdx], snap.Votes[voteIdx+1:]...)
				break // only one vote allowed
			}
		}
		// Tally up the new vote from the signer
		var authorize bool
		switch {
		case bytes.Equal(header.Nonce[:], nonceAuthVote):
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
				for voteIdx := 0; voteIdx < len(snap.Votes); voteIdx++ {
					if snap.Votes[voteIdx].Signer == header.Coinbase {
						// Uncast the vote from the cached tally
						snap.uncast(snap.Votes[voteIdx].Address, snap.Votes[voteIdx].Authorize)

						// Uncast the vote from the chronological list
						snap.Votes = append(snap.Votes[:voteIdx], snap.Votes[voteIdx+1:]...)

						voteIdx--
					}
				}
			}
			// Discard any previous votes around the just changed account
			for voteIdx := 0; voteIdx < len(snap.Votes); voteIdx++ {
				if snap.Votes[voteIdx].Address == header.Coinbase {
					snap.Votes = append(snap.Votes[:voteIdx], snap.Votes[voteIdx+1:]...)
					voteIdx--
				}
			}
			delete(snap.Tally, header.Coinbase)
		}
		// If we're taking too much time (ecrecover), notify the user once a while
		if time.Since(logged) > 8*time.Second {
			log.Info("Reconstructing voting history", "processed", i, "total", len(headers), "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}
	if time.Since(start) > 8*time.Second {
		log.Info("Reconstructed voting history", "processed", len(headers), "elapsed", common.PrettyDuration(time.Since(start)))
	}
	snap.Number += uint64(len(headers))
	snap.Hash = headers[len(headers)-1].Hash()

	return snap, nil
}

// signers retrieves the list of authorized signers in ascending order.
func (s *Snapshot) signers() []common.Address {
	sigs := make([]common.Address, 0, len(s.Signers))
	for sig := range s.Signers {
		sigs = append(sigs, sig)
	}
	sort.Sort(signersAscending(sigs))
	return sigs
}

// inturn returns if a signer at a given block height is in-turn or not.
func (s *Snapshot) inturn(number uint64, signer common.Address) bool {
	signers, offset := s.signers(), 0
	for offset < len(signers) && signers[offset] != signer {
		offset++
	}
	return (number % uint64(len(signers))) == uint64(offset)
}
