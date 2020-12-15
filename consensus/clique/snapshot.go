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
	return db.Has(dbutils.HeaderHashPrefix, enc)
}

var zeroValue [8]byte

func addSnapshotByBlock(db ethdb.Database, num uint64) error {
	enc := dbutils.EncodeBlockNumber(num)
	return db.Put(dbutils.HeaderHashPrefix, enc, zeroValue[:])
}

// store inserts the snapshot into the database.
func (s *Snapshot) store(db ethdb.Database, force bool) error {
	t := time.Now()
	ok, err := hasSnapshotData(db, s.Number, s.Hash)
	if ok && err == nil {
		return nil
	}
	fmt.Println("+++store-0.0", s.Number, s.Hash.String(), time.Since(t))

	t = time.Now()
	blob, err := json.Marshal(s)
	fmt.Println("+++store-0", s.Number, s.Hash.String(), time.Since(t), len(blob), "bytes")
	t = time.Now()
	if err != nil {
		//fmt.Printf("Snapshot.store ERROR snap %d(%s): %v\n", s.Number, s.Hash.String(), err)
		return err
	}

	//fmt.Printf("Snapshot.store snap %d(%s): %v\n", s.Number, s.Hash.String(), spew.Sdump(s.Signers))

	s.snapStorage.save(db, s.Number, s.Hash, blob, force)
	fmt.Println("+++store-1", s.Number, s.Hash.String(), time.Since(t), len(blob), "bytes")
	return err
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
func (s *Snapshot) apply(headers []*types.Header) error {
	// Allow passing in no headers for cleaner code
	if len(headers) == 0 {
		return nil
	}

	// Sanity check that the headers can be applied
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			//fmt.Println("errInvalidVotingChain1")
			return errInvalidVotingChain
		}
	}

	if headers[0].Number.Uint64() != s.Number+1 {
		//fmt.Println("errInvalidVotingChain2", headers[0].Number.Uint64(), headers[len(headers)-1].Number.Uint64(), s.Number+1)
		return errInvalidVotingChain
	}

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
			s.Votes = nil
			s.Tally = make(map[common.Address]Tally)
		}
		// Delete the oldest signer from the recent list to allow it signing again
		if limit := uint64(len(s.Signers)/2 + 1); number >= limit {
			delete(s.Recents, number-limit)
		}
		// Resolve the authorization key and check against signers
		signer, err := ecrecover(header, s.sigcache)
		if err != nil {
			return err
		}
		if _, ok := s.Signers[signer]; !ok {
			//fmt.Printf("errUnauthorizedSigner-3 for block %d\n", number)
			return errUnauthorizedSigner
		}
		for _, recent := range s.Recents {
			if recent == signer {
				//fmt.Println("recently signed2", number, b, recent.String())
				return errRecentlySigned
			}
		}
		s.Recents[number] = signer

		// Header authorized, discard any previous votes from the signer
		for voteIdx, vote := range s.Votes {
			if vote.Signer == signer && vote.Address == header.Coinbase {
				// Uncast the vote from the cached tally
				s.uncast(vote.Address, vote.Authorize)

				// Uncast the vote from the chronological list
				s.Votes = append(s.Votes[:voteIdx], s.Votes[voteIdx+1:]...)
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
			return errInvalidVote
		}
		if s.cast(header.Coinbase, authorize) {
			s.Votes = append(s.Votes, &Vote{
				Signer:    signer,
				Block:     number,
				Address:   header.Coinbase,
				Authorize: authorize,
			})
		}
		// If the vote passed, update the list of signers
		if tally := s.Tally[header.Coinbase]; tally.Votes > len(s.Signers)/2 {
			if tally.Authorize {
				s.Signers[header.Coinbase] = struct{}{}
			} else {
				delete(s.Signers, header.Coinbase)

				// Signer list shrunk, delete any leftover recent caches
				if limit := uint64(len(s.Signers)/2 + 1); number >= limit {
					delete(s.Recents, number-limit)
				}
				// Discard any previous votes the deauthorized signer cast
				for voteIdx := 0; voteIdx < len(s.Votes); voteIdx++ {
					if s.Votes[voteIdx].Signer == header.Coinbase {
						// Uncast the vote from the cached tally
						s.uncast(s.Votes[voteIdx].Address, s.Votes[voteIdx].Authorize)

						// Uncast the vote from the chronological list
						s.Votes = append(s.Votes[:voteIdx], s.Votes[voteIdx+1:]...)

						voteIdx--
					}
				}
			}
			// Discard any previous votes around the just changed account
			for voteIdx := 0; voteIdx < len(s.Votes); voteIdx++ {
				if s.Votes[voteIdx].Address == header.Coinbase {
					s.Votes = append(s.Votes[:voteIdx], s.Votes[voteIdx+1:]...)
					voteIdx--
				}
			}
			delete(s.Tally, header.Coinbase)
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
	s.Number += uint64(len(headers))
	s.Hash = headers[len(headers)-1].Hash()

	return nil
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

func (st *storage) save(db ethdb.Database, number uint64, hash common.Hash, blob []byte, force bool) {
	force = true
	st.Once.Do(func() {
		st.db = db

		st.ch = make(chan snapObj, 16384)

		go func() {
			// fixme add an exit condition
			for snap := range st.ch {
				st.saveSnap(snap)
			}
		}()
	})

	t := time.Now()
	ok, err := hasSnapshotData(st.db, number, hash)
	fmt.Println("+++snapshot-7.HAS", number, hash.String(), time.Since(t), "ok", ok, "err", err)
	if !ok || err != nil {
		fmt.Println("+++snapshot-XXX Going to send", number, hash.String())
		snap := snapObj{number, hash, blob}
		if !force {
			st.ch <- snap
		} else {
			st.saveSnap(snap)
		}

	} else {
		fmt.Println("+++snapshot-XXX NOT Going to send", number, hash.String())
	}
}

func (st *storage) saveSnap(snap snapObj) {
	t := time.Now()
	err := addSnapshotByBlock(st.db, snap.number)
	if err != nil {
		log.Error("can't store snapshot index", "db", st.db != nil, "block", snap.number, "hash", snap.hash, "err", err)
		return
	}

	fmt.Println("+++snapshot-7.2", snap.number, snap.hash.String(), time.Since(t))
	t = time.Now()
	err = st.db.Put(dbutils.CliqueBucket, dbutils.BlockBodyKey(snap.number, snap.hash), snap.blob)
	if err != nil {
		log.Error("can't store snapshot", "block", snap.number, "hash", snap.hash, "err", err)
		return
	}
	fmt.Println("+++snapshot-7.3", snap.number, snap.hash.String(), time.Since(t))
}
