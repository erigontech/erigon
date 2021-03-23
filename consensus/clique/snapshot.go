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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"

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
	config *params.CliqueConfig // Consensus engine parameters to fine tune behavior

	Number  uint64                      `json:"number"`  // Block number where the snapshot was created
	Hash    common.Hash                 `json:"hash"`    // Block hash where the snapshot was created
	Signers map[common.Address]struct{} `json:"signers"` // Set of authorized signers at this moment
	Recents map[uint64]common.Address   `json:"recents"` // Set of recent signers for spam protections
	Votes   []*Vote                     `json:"votes"`   // List of votes cast in chronological order
	Tally   map[common.Address]Tally    `json:"tally"`   // Current vote tally to avoid recalculating

	snapStorage *storage
}

// signersAscending implements the sort interface to allow sorting a list of addresses
type signersAscending []common.Address

func (s signersAscending) Len() int           { return len(s) }
func (s signersAscending) Less(i, j int) bool { return bytes.Compare(s[i][:], s[j][:]) < 0 }
func (s signersAscending) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// newSnapshot creates a new snapshot with the specified startup parameters. This
// method does not initialize the set of recent signers, so only ever use if for
// the genesis block.
func newSnapshot(config *params.CliqueConfig, snapStorage *storage, number uint64, hash common.Hash, signers []common.Address) *Snapshot {
	snap := &Snapshot{
		config:      config,
		Number:      number,
		Hash:        hash,
		Signers:     make(map[common.Address]struct{}),
		Recents:     make(map[uint64]common.Address),
		Tally:       make(map[common.Address]Tally),
		snapStorage: snapStorage,
	}
	for _, signer := range signers {
		snap.Signers[signer] = struct{}{}
	}
	return snap
}

// loadSnapshot loads an existing snapshot from the database.
func loadAndFillSnapshot(db ethdb.Database, num uint64, hash common.Hash, config *params.CliqueConfig, snapStorage *storage) (*Snapshot, error) {
	snap, err := loadSnapshot(db, num, hash)
	if err != nil {
		return nil, err
	}

	snap.config = config
	snap.snapStorage = snapStorage

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

	return snap, nil
}

func getSnapshotData(db ethdb.Database, num uint64, hash common.Hash) ([]byte, error) {
	return db.Get(dbutils.CliqueBucket, SnapshotFullKey(num, hash))
}

func hasSnapshotData(db ethdb.Database, num uint64, hash common.Hash) (bool, error) {
	return db.Has(dbutils.CliqueBucket, SnapshotFullKey(num, hash))
}

func hasSnapshot(db ethdb.Database, num uint64) (bool, error) {
	return db.Has(dbutils.CliqueSnapshotBucket, SnapshotKey(num))
}

var ErrNotFound = errors.New("not found")

func lastSnapshot(db ethdb.Database) (uint64, error) {
	lastEnc, err := db.Get(dbutils.CliqueLastSnapshotBucket, LastSnapshotKey())
	if err != nil {
		log.Error("can't check last snapshot", "err", err)
		debugLog("lastSnapshot-1", err)
		return 0, ErrNotFound
	}

	lastNum, err := dbutils.DecodeBlockNumber(lastEnc)
	if err != nil {
		log.Error("can't decode last snapshot", "err", err)
		debugLog("lastSnapshot-2", err)
		return 0, ErrNotFound
	}

	debugLog("lastSnapshot-3", lastNum)

	return lastNum, nil
}

// store inserts the snapshot into the database.
func (s *Snapshot) store(force bool) error {
	return s.snapStorage.save(s.Number, s.Hash, s, force)
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
func (s *Snapshot) apply(r *recoverer, snapID uint64, headers ...*types.Header) error {
	// Allow passing in no headers for cleaner code
	if len(headers) == 0 {
		return nil
	}

	// Sanity check that the headers can be applied
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			debugLog("invalid voting chain - 1")
			return fmt.Errorf("%w:  next block index %d(%d), previous block index %d(%d) - %d",
				errInvalidVotingChain, i+1, headers[i+1].Number.Uint64(), i, headers[i].Number.Uint64()+1, snapID)
		}
	}

	if headers[0].Number.Uint64() != s.Number+1 {
		parStr := "parents: "
		for _, par := range headers {
			parStr += fmt.Sprintf("%d ", par.Number.Uint64())
		}
		fmt.Printf("!!! invalid voting chain - 2, id=%d, from %d, to %d, snap+1 %d, %s - %d\n",
			snapID, headers[0].Number.Uint64(), headers[len(headers)-1].Number.Uint64(), s.Number+1, parStr, snapID)

		return fmt.Errorf("%w: highest parent %d, snap %d - %s - %d",
			errInvalidVotingChain, headers[0].Number.Uint64(), s.Number, parStr, snapID)
	}

	var start, logged time.Time
	if len(headers) > 1 {
		start = time.Now()
		logged = start
	}

	signers, err := r.ecrecovers(headers)
	if err != nil {
		return err
	}

	for i := range headers {
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
		signer := signers[i]
		if _, ok := s.Signers[signer]; !ok {
			return errUnauthorizedSigner
		}
		for _, recent := range s.Recents {
			if recent == signer {
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

		if len(headers) > 1 {
			// If we're taking too much time (ecrecover), notify the user once a while
			if time.Since(logged) > 8*time.Second {
				log.Info("Reconstructing voting history", "processed", i, "total", len(headers), "elapsed", common.PrettyDuration(time.Since(start)))
				logged = time.Now()
			}
		}
	}

	if len(headers) > 1 {
		if time.Since(start) > 8*time.Second {
			log.Info("Reconstructed voting history", "processed", len(headers), "elapsed", common.PrettyDuration(time.Since(start)))
		}
	}

	s.Number += uint64(len(headers))
	s.Hash = headers[len(headers)-1].HashCache()

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

func (s *Snapshot) Copy() *Snapshot {
	snap := newSnapshot(s.config, s.snapStorage, s.Number, s.Hash, s.signers())

	snap.Recents = make(map[uint64]common.Address, len(s.Recents))
	for k, v := range s.Recents {
		snap.Recents[k] = v
	}

	snap.Votes = make([]*Vote, len(s.Votes))
	copy(snap.Votes, s.Votes)

	snap.Tally = make(map[common.Address]Tally, len(s.Tally))
	for k, v := range s.Tally {
		snap.Tally[k] = v
	}

	return snap
}

type storage struct {
	ch        chan *snapObj
	chStatus  *uint32
	db        ethdb.Database
	saveMu    sync.Mutex
	exit      chan struct{}
	exitDone  chan struct{}
	batchSize int
}

type snapObj struct {
	number uint64
	hash   common.Hash
	blob   *Snapshot
}

func newStorage(db ethdb.Database, exitCh chan struct{}) *storage {
	const batchSize = 100000
	const syncSmallBatch = time.Minute

	st := &storage{
		db:        db,
		ch:        make(chan *snapObj, batchSize/2),
		chStatus:  new(uint32),
		exit:      exitCh,
		exitDone:  make(chan struct{}),
		batchSize: batchSize,
	}

	go func() {
		snaps := make([]*snapObj, 0, batchSize)
		syncSmall := time.NewTicker(syncSmallBatch)
		isSorted := true

		defer func() {
			syncSmall.Stop()
			common.SafeClose(st.exitDone)
		}()

		for {
			select {
			case snap := <-st.ch:
				debugLog("to-save-1", snap.number)
				snaps, isSorted = st.appendSnap(snap, snaps, isSorted)

				if len(snaps) >= batchSize {
					snaps, isSorted = st.saveAndReset(snaps, isSorted)
					syncSmall.Reset(syncSmallBatch)
				}
			case <-syncSmall.C:
				if len(snaps) > 0 && len(snaps) < int(syncSmallBatch.Seconds()) {
					snaps, isSorted = st.saveAndReset(snaps, isSorted)
				}
			case <-st.exit:
				for snap := range st.ch {
					snaps, isSorted = st.appendSnap(snap, snaps, isSorted)
				}

				if len(snaps) > 0 {
					st.saveSnaps(snaps, isSorted)
				}

				return
			}
		}
	}()

	return st
}

func (st *storage) saveAndReset(snaps []*snapObj, isSorted bool) ([]*snapObj, bool) {
	st.saveSnaps(snaps, isSorted)
	snaps = snaps[:0]
	isSorted = true

	return snaps, isSorted
}

func (st *storage) appendSnap(snap *snapObj, snaps []*snapObj, isSorted bool) ([]*snapObj, bool) {
	if st.shallAppend(snap) {
		snaps = append(snaps, snap)

		if isSorted && len(snaps) > 1 && snaps[len(snaps)-2].number > snap.number {
			isSorted = false
		}
	}
	return snaps, isSorted
}

func (st *storage) save(number uint64, hash common.Hash, s *Snapshot, force bool) error {
	if !force {
		if atomic.LoadUint32(st.chStatus) == 1 {
			return nil
		}
		select {
		case <-st.exit:
		case st.ch <- &snapObj{number, hash, s}:
		}

		return nil
	}

	debugLog("SAVE-1===================================================", number, debug.Callers(10))

	// a forced case (genesis)
	ok, err := hasSnapshotData(st.db, number, hash)

	debugLog("to-save-2", number, ok, err)
	if !ok || err != nil {
		debugLog("to-save-2.0", number)
		if err != nil {
			debugLog("error while hasSnapshotData",
				"number", number,
				"hash", hash.String(),
				"error", err,
				"has", ok)
		}
		debugLog("to-save-2.1", number)
		blob, err := json.Marshal(s)
		if err != nil {
			debugLog("to-save-2.2", number, err)
			return err
		}
		debugLog("to-save-2.1.1", number)

		st.saveMu.Lock()
		defer st.saveMu.Unlock()
		tx, err := st.db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			debugLog("to-save-2.3", number, err)
			return err
		}
		debugLog("to-save-2.1.2", number)
		defer tx.Rollback()

		if err := tx.Put(dbutils.CliqueBucket, SnapshotFullKey(number, hash), blob); err != nil {
			log.Error("can't store a snapshot", "block", number, "hash", hash, "err", err)
			debugLog("to-save-2.4", number, err)
			return err
		}
		debugLog("to-save-2.1.3", number)

		if err := tx.Put(dbutils.CliqueSnapshotBucket, SnapshotKey(number), []byte{0}); err != nil {
			log.Error("can't store a snapshot number", "block", number, "hash", hash, "err", err)
			debugLog("to-save-2.5", number, err)
			return err
		}
		debugLog("to-save-2.1.4", number)

		lastSnap, err := lastSnapshot(tx)
		if lastSnap < number || errors.Is(err, ErrNotFound) {
			debugLog("to-save-2.6", number, err)
			if err := tx.Put(dbutils.CliqueLastSnapshotBucket, LastSnapshotKey(), dbutils.EncodeBlockNumber(number)); err != nil {
				log.Error("can't store a snapshot number", "block", number, "hash", hash, "err", err)
				debugLog("to-save-2.7", number, err)
				return err
			}
		}
		debugLog("to-save-2.1.5", number)

		if err := tx.Commit(); err != nil {
			log.Error("can't commit snapshot transaction", "block", number, "hash", hash, "err", err)
			debugLog("to-save-2.8", number, err)
			return err
		}
		debugLog("to-save-2.1.6", number)

		debugLog("COMMITTED-1", number)
	}

	debugLog("SAVE-1.1===================================================")

	return nil
}

func (st *storage) saveSnaps(snaps []*snapObj, isSorted bool) {
	debugLog("SAVE-2", len(snaps))
	if len(snaps) == 0 {
		return
	}

	if !isSorted && len(snaps) > 1 {
		sort.SliceStable(snaps, func(i, j int) bool {
			return snaps[i].number < snaps[j].number ||
				(snaps[i].number == snaps[j].number && bytes.Compare(snaps[i].hash[:], snaps[j].hash[:]) == -1)
		})
	}

	st.saveMu.Lock()
	defer st.saveMu.Unlock()

	batch := st.db.NewBatch()
	defer batch.Rollback()

	var blob []byte

	for _, snap := range snaps {
		ok, err := hasSnapshotData(batch, snap.number, snap.hash)
		if ok && err == nil {
			debugLog("save-snap-ch-1", snap.number, err)
			continue
		}

		blob, err = json.Marshal(snap)
		if err != nil {
			debugLog("save-snap-ch-2", snap.number, err)
			log.Error("can't store a snapshot(marshalling)", "block", snap.number, "hash", snap.hash, "err", err)
			continue
		}

		if err := batch.Put(dbutils.CliqueBucket, SnapshotFullKey(snap.number, snap.hash), blob); err != nil {
			debugLog("save-snap-ch-3", snap.number, err)
			log.Error("can't store a snapshot", "block", snap.number, "hash", snap.hash, "err", err)
		}

		if err := batch.Put(dbutils.CliqueSnapshotBucket, SnapshotKey(snap.number), []byte{0}); err != nil {
			debugLog("save-snap-ch-4", snap.number, err)
			log.Error("can't store a snapshot number", "block", snap.number, "hash", snap.hash, "err", err)
		}

		lastSnap, err := lastSnapshot(batch)
		if lastSnap < snap.number || errors.Is(err, ErrNotFound) {
			debugLog("save-snap-ch-5", snap.number, err)
			if err := batch.Put(dbutils.CliqueLastSnapshotBucket, LastSnapshotKey(), dbutils.EncodeBlockNumber(snap.number)); err != nil {
				debugLog("save-snap-ch-6", snap.number, err)
				log.Error("can't store a snapshot number", "block", snap.number, "hash", snap.hash, "err", err)
			}
		}
	}

	if err := batch.Commit(); err != nil {
		debugLog("save-snap-ch-7", err)
		log.Error("can't store snapshots", "blockFrom", snaps[0].number, "blockTo", snaps[len(snaps)-1].number, "err", err)
		return
	}

	debugLog("COMMITTED-2", snaps[0].number, snaps[len(snaps)-1].number)
}

func (st *storage) Close() {
	if atomic.CompareAndSwapUint32(st.chStatus, 0, 1) {
		close(st.ch)
	}

	<-st.exitDone
	st.db.Close()
}

func (st *storage) shallAppend(snap *snapObj) bool {
	if snap == nil {
		return false
	}
	ok, err := hasSnapshotData(st.db, snap.number, snap.hash)
	return !ok || err != nil
}
