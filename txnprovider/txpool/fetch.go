// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	libkzg "github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

// errInternalDB wraps errors that originate from local DB lookups (not from the peer's data).
// This prevents penalizing peers for our own internal failures.
var errInternalDB = errors.New("internal db error")

// txFetchTimeout is how long an announced hash waits for the requested peer before another announcer is asked.
const txFetchTimeout = 5 * time.Second

// maxAlternatesPerPeer bounds the hashes a peer waits on as an alternate, as geth's maxTxAnnounces does: a hash
// keeps every announcer, so silent peers that announce first cannot crowd out a later one.
const maxAlternatesPerPeer = 4096

const maxFetchRequests = 64 * 1024

// disconnectedPeerMemory is longer than an announcement of a peer waits in the inbound queue.
const disconnectedPeerMemory = time.Minute

// maxRetryRequestsPerPeer bounds a peer's unanswered retry requests well under sentry's 32-message outbound queue,
// which drops the oldest when full: retries alone cannot fill it, and a slow writer gets no more until it answers.
// Other traffic shares the queue, so a retry can still be dropped; retryAnswerTimeout frees its slot.
const maxRetryRequestsPerPeer = 8

// initialRequestID marks requests that hold no retry slot. Each retry gets its own larger id, and a reply frees a slot
// only by echoing the id of a retry still outstanding.
const initialRequestID uint64 = 1

type retrySlot struct {
	id   uint64
	sent time.Time
}

// retryAnswerTimeout frees the slot of a retry left unanswered this long. A full 32-message sentry queue drains
// within this time at the 20-second write timeout, so an older retry was written or dropped and is not queued.
const retryAnswerTimeout = 32 * 20 * time.Second

type fetchPeer struct {
	sentry sentryproto.SentryClient
	peerID [64]byte
}

type fetchRequest struct {
	at         time.Time
	peer       fetchPeer
	alternates []fetchPeer
	unsent     bool // reserved for peer, but its request is not sent yet, so it cannot time out
}

type pendingFetch struct {
	hash [32]byte
	at   time.Time
}

// announceInfo records the (type, size) that a peer promised for a given hash
// in an eth/68 NewPooledTransactionHashes announcement. We keep the peer id so
// that a later PooledTransactions reply from a different peer does not clobber
// the check (two peers can legitimately announce the same hash).
type announceInfo struct {
	peerHash uint64 // maphash of the announcing peer's 64-byte id
	txnType  byte
	size     uint32
}

type txnChange struct {
	txn       *TxnSlot
	sender    [20]byte
	direction remoteproto.Direction
}

// Fetch connects to sentry and implements eth/66 protocol regarding the transaction
// messages. It tries to "prime" the sentry with StatusData message containing given
// genesis hash and list of forks, but with zero max block and total difficulty
// Sentry should have a logic not to overwrite statusData with messages from txn pool
type Fetch struct {
	ctx                      context.Context // Context used for cancellation and closing of the fetcher
	pool                     Pool            // Transaction pool implementation
	db                       kv.RwDB
	stateChangesClient       StateChangesClient
	pendingTxnChanges        []txnChange     // Owned by ConnectCore; retained across subscriptions until applied
	wg                       *sync.WaitGroup // used for synchronisation in the tests (nil when not in tests)
	connectWg                sync.WaitGroup  // tracks goroutines spawned by ConnectCore/ConnectSentries
	stateChangesParseCtx     *TxnParseContext
	pooledTxnsParseCtx       *TxnParseContext
	sentryClients            []sentryproto.SentryClient // sentry clients that will be used for accessing the network
	stateChangesParseCtxLock sync.Mutex
	pooledTxnsParseCtxLock   sync.Mutex
	announcements            *maphash.LRU[announceInfo] // announceKey(txHash, peerHash) -> announced (type, size) from that peer
	requestedLock            sync.Mutex
	requested                *maphash.LRU[*fetchRequest]
	pending                  []pendingFetch            // in request order, so the oldest expires first
	waiting                  []pendingFetch            // timed-out requests whose alternates have no retry room
	waitingReady             bool                      // a waiting request's alternate may have room again
	retriesOutstanding       map[fetchPeer][]retrySlot // the retries a peer has not answered, oldest first
	lastRetryID              uint64
	alternatesOf             map[fetchPeer]int       // hashes each peer waits on as an alternate
	disconnected             map[fetchPeer]time.Time // recently disconnected peers: an announcement buffered before the disconnect or a failed send must not keep them
	now                      func() time.Time
	newBlocks                atomic.Uint64
	logger                   log.Logger
}

type StateChangesClient interface {
	StateChanges(ctx context.Context, in *remoteproto.StateChangeRequest, opts ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error)
}

// NewFetch creates a new fetch object that will work with given sentry clients. Since the
// SentryClient here is an interface, it is suitable for mocking in tests (mock will need
// to implement all the functions of the SentryClient interface).
func NewFetch(
	ctx context.Context,
	sentryClients []sentryproto.SentryClient,
	pool Pool,
	stateChangesClient StateChangesClient,
	db kv.RwDB,
	chainID uint256.Int,
	logger log.Logger,
	opts ...Option,
) *Fetch {
	options := applyOpts(opts...)
	// 64k entries is roughly 1–2 hours of mainnet announcement traffic — more
	// than the fetcher's own working window (announce → POOLED_TRANSACTIONS
	// reply is typically seconds, at worst minutes), and bounded so a DoSing
	// peer spamming unique hashes can't grow this unboundedly.
	announcements, _ := maphash.NewLRU[announceInfo](64 * 1024)
	requested, _ := maphash.NewLRU[*fetchRequest](maxFetchRequests)
	f := &Fetch{
		ctx:                  ctx,
		sentryClients:        sentryClients,
		pool:                 pool,
		db:                   db,
		stateChangesClient:   stateChangesClient,
		stateChangesParseCtx: NewTxnParseContext(chainID).ChainIDRequired(), // TODO: change ctx if rules changed
		pooledTxnsParseCtx:   NewTxnParseContext(chainID).ChainIDRequired().WithPoolRules(),
		announcements:        announcements,
		requested:            requested,
		retriesOutstanding:   map[fetchPeer][]retrySlot{},
		lastRetryID:          initialRequestID,
		alternatesOf:         map[fetchPeer]int{},
		disconnected:         map[fetchPeer]time.Time{},
		now:                  time.Now,
		wg:                   options.p2pFetcherWg,
		logger:               logger,
	}
	f.pooledTxnsParseCtx.ValidateRLP(ValidateSerializedTxn)
	if pool != nil {
		f.pooledTxnsParseCtx.RejectPrefix(pool.RejectFramePrefix)
	}
	f.stateChangesParseCtx.ValidateRLP(ValidateSerializedTxn)

	return f
}

// peerHash maps a peer's H512 id to the 64-bit hash we use as part of the
// announcements LRU key and as a sanity check on lookup. The check is
// probabilistic (64-bit birthday ~4B peers before a collision is likely) —
// fine in practice, the worst case is a missed kick for a different peer
// that hash-collides with the announcer.
func peerHash(pid *typesproto.H512) uint64 {
	b := gointerfaces.ConvertH512ToHash(pid)
	return maphash.Hash(b[:])
}

// announceKey builds the composite (txHash, peerHash) LRU key used to record
// and look up announcements. Keying by peer as well as hash prevents a second
// announcer of the same hash from clobbering the first's entry — without it,
// a later benign announcement can exonerate an earlier lying peer by making
// checkPooledTxnAnnouncement bail out on a mismatched peerHash.
func announceKey(txHash []byte, peerHash uint64) [40]byte {
	var k [40]byte
	copy(k[:32], txHash)
	binary.BigEndian.PutUint64(k[32:], peerHash)
	return k
}

// skipRecentlyRequested drops hashes already requested less than txFetchTimeout ago and keeps
// the announcer as an alternate for them.
func (f *Fetch) skipRecentlyRequested(hashes Hashes, sentryClient sentryproto.SentryClient, peerID *typesproto.H512) (Hashes, time.Time) {
	f.requestedLock.Lock()
	defer f.requestedLock.Unlock()
	now := f.now()
	peer := fetchPeer{sentryClient, gointerfaces.ConvertH512ToHash(peerID)}
	var toRequest Hashes
	for i := 0; i < len(hashes); i += 32 {
		h := hashes[i : i+32]
		r, ok := f.requested.Get(h)
		if ok && (now.Sub(r.at) < txFetchTimeout || len(r.alternates) > 0 || r.unsent) {
			f.addAlternate(r, peer) // a timed-out request asks its waiting alternates first
			continue
		}
		// An announcement flood: forget the oldest requests before the insert, so a full LRU evicts no other
		// pending request, and a forgotten hash is asked again on its next announcement. A restamped retry
		// leaves a stale entry that frees no slot, so trim until the queue is below the cache size.
		for len(f.pending)+len(f.waiting) >= maxFetchRequests {
			oldest := &f.pending
			if len(f.waiting) > 0 {
				oldest = &f.waiting
			}
			if old, found := f.requested.Get((*oldest)[0].hash[:]); found && old.at.Equal((*oldest)[0].at) {
				f.forget((*oldest)[0].hash[:], old)
			}
			*oldest = (*oldest)[1:]
			if ok {
				r, ok = f.requested.Get(h)
			}
		}
		if ok {
			r.at, r.peer, r.unsent = now, peer, true
		} else {
			f.requested.Set(h, &fetchRequest{at: now, peer: peer, unsent: true})
		}
		f.pending = append(f.pending, pendingFetch{[32]byte(h), now})
		toRequest = append(toRequest, h...)
	}
	return toRequest, now
}

// retryLoop runs the one timeout sweep shared by all sentry streams.
func (f *Fetch) retryLoop() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			f.retryTimedOutRequests(f.ctx)
		}
	}
}

func (f *Fetch) retryTimedOutRequests(ctx context.Context) {
	if f.db == nil || !f.pool.Started() {
		return
	}
	tx, err := f.db.BeginRo(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()
	f.requestFromAlternates(ctx, tx)
}

// retryPlan groups a sweep's retries for one peer into requests a responder answers in full.
type retryPlan struct {
	firstID uint64
	reqs    []Hashes
	size    int // announced bytes in the last request
	limit   int // requests the peer's budget still allows
}

// add reports false when the hash needs a request beyond the peer's remaining budget (limit).
func (p *retryPlan) add(hash []byte, size int) bool {
	if n := len(p.reqs); n == 0 || len(p.reqs[n-1]) == 256*32 || p.size >= p2pTxPacketLimit {
		if n == p.limit {
			return false
		}
		p.reqs, p.size = append(p.reqs, nil), 0
	}
	p.reqs[len(p.reqs)-1] = append(p.reqs[len(p.reqs)-1], hash...)
	p.size += size
	return true
}

// announcedSize is the size the peer announced for the hash. An unknown size counts as a full
// response, since it may be a blob.
func (f *Fetch) announcedSize(hash []byte, peer fetchPeer) int {
	if f.announcements == nil {
		return p2pTxPacketLimit
	}
	k := announceKey(hash, peerHash(gointerfaces.ConvertHashToH512(peer.peerID)))
	info, ok := f.announcements.Get(k[:])
	if !ok {
		return p2pTxPacketLimit
	}
	return int(info.size) + announcedSizeSlack // a delivered body may be that much larger
}

// requestFromAlternates asks the next announcer for each hash whose request timed out. Hashes the pool holds
// leave the retry state, queued ones wait for their validation, and a rejected body keeps its alternates.
func (f *Fetch) requestFromAlternates(ctx context.Context, tx kv.Tx) {
	f.requestedLock.Lock()
	now := f.now()
	f.expireRetries(now)
	scanWaiting := f.waitingReady
	var expired Hashes
	if scanWaiting {
		f.waitingReady = false
		kept := f.waiting[:0]
		for _, p := range f.waiting {
			r, ok := f.requested.Get(p.hash[:])
			if !ok || !r.at.Equal(p.at) {
				continue // stale
			}
			kept = append(kept, p)
			if len(r.alternates) == 0 || f.hasRetryRoom(r) { // an exhausted hash is looked up too: its body may be queued
				expired = append(expired, p.hash[:]...)
			}
		}
		f.waiting = kept
	}
	for _, p := range f.pending {
		if now.Sub(p.at) < txFetchTimeout {
			break
		}
		expired = append(expired, p.hash[:]...)
	}
	f.requestedLock.Unlock()
	if len(expired) == 0 {
		return
	}
	state := map[[32]byte]bool{} // true: held, false: queued for validation
	checked := make(map[[32]byte]struct{}, len(expired)/32)
	for i := 0; i < len(expired); i += 32 {
		checked[[32]byte(expired[i:i+32])] = struct{}{}
	}
	held, queued, err := f.pool.HeldOrQueuedIdHashes(tx, expired)
	if err != nil {
		held, queued = nil, nil // ask anyway: a held txn costs only a duplicate reply
	}
	for i := 0; i < len(held); i += 32 {
		state[[32]byte(held[i:i+32])] = true
	}
	for i := 0; i < len(queued); i += 32 {
		state[[32]byte(queued[i:i+32])] = false
	}

	f.requestedLock.Lock()
	sent := f.now() // the lookup may have been slow; retries time out from when they are sent
	plans := map[fetchPeer]*retryPlan{}
	var deferred []pendingFetch
	waiting, timedOut := f.waiting, []pendingFetch(nil)
	if scanWaiting {
		waiting, timedOut = nil, f.waiting
	}
	for len(timedOut) > 0 || len(f.pending) > 0 && now.Sub(f.pending[0].at) >= txFetchTimeout {
		var p pendingFetch
		fromWaiting := len(timedOut) > 0
		if fromWaiting {
			p, timedOut = timedOut[0], timedOut[1:]
		} else {
			p, f.pending = f.pending[0], f.pending[1:]
		}
		r, ok := f.requested.Get(p.hash[:])
		if !ok || !r.at.Equal(p.at) {
			continue
		}
		if r.unsent { // it times out from its send; until then it keeps its entry for cleanup and admission
			deferred = append(deferred, p)
			continue
		}
		if _, lookedUp := checked[p.hash]; fromWaiting && (!lookedUp || len(r.alternates) > 0 && !f.hasRetryRoom(r)) {
			waiting = append(waiting, p) // room that opened during the unlocked lookup waits for the next sweep's lookup
			continue
		}
		if isHeld, found := state[p.hash]; found {
			if isHeld {
				f.forget(p.hash[:], r)
				continue
			}
			r.at = sent // a queued body may still fail validation; keep the alternates
			f.pending = append(f.pending, pendingFetch{p.hash, sent})
			continue
		}
		if len(r.alternates) == 0 {
			f.requested.Delete(p.hash[:]) // exhausted: free the slot for live requests
			continue
		}
		picked := slices.IndexFunc(r.alternates, func(alt fetchPeer) bool {
			plan := plans[alt]
			if plan == nil {
				plan = &retryPlan{limit: maxRetryRequestsPerPeer - len(f.retriesOutstanding[alt])}
				plans[alt] = plan
			}
			return plan.add(p.hash[:], f.announcedSize(p.hash[:], alt))
		})
		if picked < 0 { // no alternate has retry room: wait until one answers
			waiting = append(waiting, p)
			continue
		}
		r.at, r.peer, r.unsent = sent, f.removeAlternate(r, picked), true
		f.pending = append(f.pending, pendingFetch{p.hash, sent})
	}
	if len(deferred) > 0 {
		f.pending = append(deferred, f.pending...)
	}
	f.waiting = waiting
	for peer, plan := range plans { // counted before the sends, so a reply that beats its send's return frees a slot
		plan.firstID = f.lastRetryID + 1
		for range plan.reqs {
			f.lastRetryID++
			f.retriesOutstanding[peer] = append(f.retriesOutstanding[peer], retrySlot{f.lastRetryID, sent})
		}
	}
	f.requestedLock.Unlock()

	// One goroutine per sentry, under one deadline, so a stalled sentry neither holds the others nor the next sweep
	// for long; after the deadline its remaining sends fail at once and their hashes keep the peer as an alternate.
	bySentry := map[sentryproto.SentryClient][]fetchPeer{}
	for peer := range plans {
		bySentry[peer.sentry] = append(bySentry[peer.sentry], peer)
	}
	var wg sync.WaitGroup
	for _, peers := range bySentry {
		wg.Go(func() {
			sendCtx, cancel := context.WithTimeout(ctx, txFetchTimeout)
			defer cancel()
			for _, peer := range peers {
				for i, hashes := range plans[peer].reqs {
					id := plans[peer].firstID + uint64(i)
					unsent, at, n, err := f.requestPooledTxns(sendCtx, peer, id, hashes, sent)
					if n == 0 {
						f.releaseRetry(peer, id)
					}
					if err != nil {
						f.logger.Debug("[txpool.fetch] request from alternate", "err", err)
						if errors.Is(err, errPeerGone) {
							f.dropAlternate(peer) // its Disconnect event may have been lost
						} else {
							f.keepAlternate(peer, unsent, at)
						}
					}
				}
			}
		})
	}
	wg.Wait()
}

// claimUnsent keeps the hashes still reserved and unsent for peer at reservedAt, marks them sent and restamps
// them, so their timeout runs from this send.
func (f *Fetch) claimUnsent(peer fetchPeer, hashes Hashes, reservedAt time.Time) (Hashes, time.Time) {
	f.requestedLock.Lock()
	defer f.requestedLock.Unlock()
	now := f.now()
	var claimed Hashes
	for i := 0; i < len(hashes); i += 32 {
		r, ok := f.requested.Get(hashes[i : i+32])
		if !ok || r.peer != peer || !r.at.Equal(reservedAt) || !r.unsent {
			continue
		}
		r.unsent = false
		if !now.Equal(reservedAt) {
			r.at = now
			f.pending = append(f.pending, pendingFetch{[32]byte(hashes[i : i+32]), now})
		}
		claimed = append(claimed, hashes[i:i+32]...)
	}
	return claimed, now
}

// releaseRequested forgets hashes whose request was not sent and that have no alternate to retry, so the
// next announcement asks at once; hashes with alternates retry one after the timeout.
func (f *Fetch) releaseRequested(peer fetchPeer, hashes Hashes) {
	f.requestedLock.Lock()
	defer f.requestedLock.Unlock()
	for i := 0; i < len(hashes); i += 32 {
		if r, ok := f.requested.Get(hashes[i : i+32]); ok && r.peer == peer && len(r.alternates) == 0 {
			f.requested.Delete(hashes[i : i+32])
		}
	}
}

// keepAlternate puts a peer whose retry request was not sent back among the hashes' alternates, even at its
// quota: it held that slot before the send.
func (f *Fetch) keepAlternate(peer fetchPeer, hashes Hashes, sent time.Time) {
	f.requestedLock.Lock()
	defer f.requestedLock.Unlock()
	if _, gone := f.disconnected[peer]; gone {
		return
	}
	for i := 0; i < len(hashes); i += 32 {
		if r, ok := f.requested.Get(hashes[i : i+32]); ok && r.peer == peer && r.at.Equal(sent) && !slices.Contains(r.alternates, peer) {
			r.alternates = append(r.alternates, peer)
			f.alternatesOf[peer]++
		}
	}
}

// errPeerGone: sentry no longer knows the peer, so a request to it reaches no one.
var errPeerGone = errors.New("peer gone")

// keepAnnouncersOfTracked keeps the announcer of a hash the pool already knows but a request still tracks (its body
// may yet fail validation) as an alternate, without a request. It returns the hashes a reply can still come back
// for, in announcement order: the unknown ones and those tracked ones.
func (f *Fetch) keepAnnouncersOfTracked(hashes, unknown Hashes, sentryClient sentryproto.SentryClient, peerID *typesproto.H512) Hashes {
	peer := fetchPeer{sentryClient, gointerfaces.ConvertH512ToHash(peerID)}
	f.requestedLock.Lock()
	defer f.requestedLock.Unlock()
	var keep Hashes
	for i, j := 0, 0; i < len(hashes); i += 32 {
		h := hashes[i : i+32]
		if j < len(unknown) && bytes.Equal(unknown[j:j+32], h) {
			keep, j = append(keep, h...), j+32
			continue
		}
		if r, ok := f.requested.Get(h); ok {
			f.addAlternate(r, peer)
			keep = append(keep, h...)
		}
	}
	return keep
}

// addAlternate keeps an announcer to ask after the requested peer.
func (f *Fetch) addAlternate(r *fetchRequest, peer fetchPeer) {
	if _, gone := f.disconnected[peer]; peer == r.peer || gone || slices.Contains(r.alternates, peer) || f.alternatesOf[peer] >= maxAlternatesPerPeer {
		return
	}
	r.alternates = append(r.alternates, peer)
	f.alternatesOf[peer]++
	if len(f.waiting) > 0 && len(f.retriesOutstanding[peer]) < maxRetryRequestsPerPeer {
		f.waitingReady = true // a waiting request may have gained an alternate with room
	}
}

func (f *Fetch) removeAlternate(r *fetchRequest, i int) fetchPeer {
	peer := r.alternates[i]
	r.alternates = slices.Delete(r.alternates, i, i+1)
	if f.alternatesOf[peer]--; f.alternatesOf[peer] <= 0 {
		delete(f.alternatesOf, peer)
	}
	return peer
}

// forget drops a request and its alternates.
func (f *Fetch) forget(hash []byte, r *fetchRequest) {
	for len(r.alternates) > 0 {
		f.removeAlternate(r, len(r.alternates)-1)
	}
	f.requested.Delete(hash)
}

// requestPooledTxns sends the hashes reserved for peer at reservedAt in requests of 256, because a responder
// answers only the first 256. Each request claims its hashes when it is sent, so an earlier stalled send cannot
// make a later one stale. On a failed send it returns the hashes not sent and when they were claimed.
func (f *Fetch) requestUnknown(hashes Hashes, reservedAt time.Time, peer fetchPeer) error {
	if len(hashes) == 0 {
		return nil
	}
	sendCtx, cancel := context.WithTimeout(f.ctx, txFetchTimeout) // a blocked send must not strand later chunks as unsent
	defer cancel()
	unsent, _, _, err := f.requestPooledTxns(sendCtx, peer, initialRequestID, hashes, reservedAt)
	if err != nil {
		f.releaseRequested(peer, unsent)
	}
	return err
}

func (f *Fetch) requestPooledTxns(ctx context.Context, peer fetchPeer, requestID uint64, hashes Hashes, reservedAt time.Time) (unsent Hashes, at time.Time, sent int, err error) {
	for start := 0; start < len(hashes); start += 256 * 32 {
		end := min(start+256*32, len(hashes))
		var chunk Hashes
		chunk, at = f.claimUnsent(peer, hashes[start:end], reservedAt)
		if len(chunk) == 0 {
			continue
		}
		var encodedRequest []byte
		encodedRequest, err = EncodeGetPooledTransactions66(chunk, requestID, nil)
		if err == nil {
			var sentTo *sentryproto.SentPeers
			sentTo, err = peer.sentry.SendMessageById(ctx, &sentryproto.SendMessageByIdRequest{
				Data:   &sentryproto.OutboundMessageData{Id: sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66, Data: encodedRequest},
				PeerId: gointerfaces.ConvertHashToH512(peer.peerID),
			}, &grpc.EmptyCallOption{})
			if err == nil && len(sentTo.GetPeers()) == 0 {
				err = errPeerGone
			}
		}
		if err != nil {
			rest, _ := f.claimUnsent(peer, hashes[end:], reservedAt)
			return append(chunk, rest...), at, sent, err
		}
		sent++
	}
	return nil, time.Time{}, sent, nil
}

// hasRetryRoom reports whether one of r's alternates can take another retry request; caller holds requestedLock.
func (f *Fetch) hasRetryRoom(r *fetchRequest) bool {
	return slices.ContainsFunc(r.alternates, func(alt fetchPeer) bool { return len(f.retriesOutstanding[alt]) < maxRetryRequestsPerPeer })
}

// expireRetries frees the slots of retries unanswered for retryAnswerTimeout. Caller holds requestedLock.
func (f *Fetch) expireRetries(now time.Time) {
	for peer, sent := range f.retriesOutstanding {
		live := slices.IndexFunc(sent, func(s retrySlot) bool { return now.Sub(s.sent) < retryAnswerTimeout })
		switch {
		case live == 0:
			continue
		case live < 0:
			delete(f.retriesOutstanding, peer)
		default:
			f.retriesOutstanding[peer] = sent[live:]
		}
		f.waitingReady = true
	}
}

// releaseRetry frees one of peer's retry slots: on a reply to a retry, which a peer sends for every request it reads,
// or for a retry that was not sent. Waiting requests may go to the peer again.
func (f *Fetch) releaseRetry(peer fetchPeer, id uint64) {
	f.requestedLock.Lock()
	defer f.requestedLock.Unlock()
	slots := f.retriesOutstanding[peer]
	i := slices.IndexFunc(slots, func(s retrySlot) bool { return s.id == id })
	if i < 0 {
		return
	}
	if len(slots) == 1 {
		delete(f.retriesOutstanding, peer)
	} else {
		f.retriesOutstanding[peer] = slices.Delete(slots, i, i+1)
	}
	f.waitingReady = true
}

// recordAnnouncement remembers the (type, size) a peer promised for each
// announced hash that also appears in `filter`. Used later to detect peers
// that lie in their announcements (see checkPooledTxnAnnouncement).
//
// `filter` must be a subset of `hashes` preserving order — pass the result of
// FilterKnownIdHashes so we only record entries for txs we're about to fetch;
// entries for already-known hashes would otherwise sit unused in the LRU
// until evicted, displacing announcements we actually care about. Pass nil
// to record every hash.
func (f *Fetch) recordAnnouncement(pid *typesproto.H512, txTypes []byte, sizes []uint32, hashes, filter []byte) {
	if f.announcements == nil || len(txTypes) == 0 {
		return
	}
	ph := peerHash(pid)
	const hashSize = 32
	fPos := 0
	for i := range txTypes {
		h := hashes[i*hashSize : (i+1)*hashSize]
		if filter != nil {
			if fPos >= len(filter) || !bytes.Equal(h, filter[fPos:fPos+hashSize]) {
				continue
			}
			fPos += hashSize
		}
		k := announceKey(h, ph)
		f.announcements.Set(k[:], announceInfo{peerHash: ph, txnType: txTypes[i], size: sizes[i]})
	}
}

// announcedSizeSlack is the wiggle-room we allow between the size a peer
// announces in eth/68 NewPooledTransactionHashes and the size it later
// delivers. Matches go-ethereum's inline threshold of 8 in
// eth/fetcher/tx_fetcher.go (|announced-delivered| > 8 drops the peer):
// typed-tx RLP vs consensus-format size accounting has off-by-a-few-bytes
// quirks in the wild, so a strict equality check produces false positives.
// The devp2p BlobViolations hive test probes with a +10 byte mismatch, so
// raising this above 8 silently disables the size check for that scenario.
const announcedSizeSlack = 8

// checkPooledTxnAnnouncement returns an error if the peer delivering `slot`
// announced it earlier with a different type or a grossly different size
// (see announcedSizeSlack). Unannounced txs (including those announced by a
// different peer) are skipped — only a self-contradicting announcement is a
// violation.
func (f *Fetch) checkPooledTxnAnnouncement(pid *typesproto.H512, slot *TxnSlot) error {
	if f.announcements == nil {
		return nil
	}
	ph := peerHash(pid)
	k := announceKey(slot.IDHash[:], ph)
	info, ok := f.announcements.Get(k[:])
	if !ok {
		return nil
	}
	// Defence against a maphash collision — peer is part of the key, so on
	// a clean hit info.peerHash must equal ph.
	if info.peerHash != ph {
		return nil
	}
	if info.txnType != slot.TxType() {
		return fmt.Errorf("announced tx type %d != actual %d for hash %x", info.txnType, slot.TxType(), slot.IDHash[:])
	}
	sizeDiff := int64(info.size) - int64(slot.Size)
	if sizeDiff < -announcedSizeSlack || sizeDiff > announcedSizeSlack {
		return fmt.Errorf("announced tx size %d != actual %d for hash %x", info.size, slot.Size, slot.IDHash[:])
	}
	// One-shot: drop the entry so the same announcement can't be replayed.
	f.announcements.Delete(k[:])
	return nil
}

func (f *Fetch) penalizePeer(ctx context.Context, sentryClient sentryproto.SentryClient, peerID *typesproto.H512) {
	if _, err := sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: peerID, Penalty: sentryproto.PenaltyKind_Kick}); err != nil {
		f.logger.Debug("[txpool] penalize peer failed", "peer", peerID, "err", err)
	}
}

// checkBlobSidecar verifies EIP-4844 per-commitment invariants on a blob tx
// wrapper that arrived from a peer. Returns an error when the sidecar's
// commitments don't match the tx's blob_versioned_hashes — the peer that
// delivered it gets penalized. Non-blob txs pass through unchecked.
//
// Callers must parse with wrappedWithBlobs=true (as TRANSACTIONS_66 and
// POOLED_TRANSACTIONS_66 do), so a blob tx reaching this check is expected
// to carry a sidecar. A blob tx with an empty sidecar is treated as a
// protocol violation via the blob_versioned_hashes / BlobBundles count
// mismatch below (blob txs must declare >=1 blob per EIP-4844).
func (f *Fetch) checkBlobSidecar(slot *TxnSlot) error {
	if slot == nil || slot.Txn == nil || (slot.Txn.Type() != types.BlobTxType && slot.Txn.GetBlobGas() == 0) {
		return nil
	}
	blobHashes := slot.Txn.GetBlobHashes()
	if len(blobHashes) != len(slot.BlobBundles) {
		return fmt.Errorf("blob_versioned_hashes count %d != blobs count %d", len(blobHashes), len(slot.BlobBundles))
	}
	for i := range slot.BlobBundles {
		if libkzg.KZGToVersionedHash(slot.BlobBundles[i].Commitment) != libkzg.VersionedHash(blobHashes[i]) {
			return fmt.Errorf("commitment[%d] does not match blob_versioned_hash[%d]", i, i)
		}
	}
	return nil
}

func (f *Fetch) threadSafeParsePooledTxn(cb func(*TxnParseContext) error) error {
	f.pooledTxnsParseCtxLock.Lock()
	defer f.pooledTxnsParseCtxLock.Unlock()
	return cb(f.pooledTxnsParseCtx)
}

func (f *Fetch) threadSafeParseStateChangeTxn(cb func(*TxnParseContext) error) error {
	f.stateChangesParseCtxLock.Lock()
	defer f.stateChangesParseCtxLock.Unlock()
	return cb(f.stateChangesParseCtx)
}

// ConnectSentries initialises connection to the sentry
func (f *Fetch) ConnectSentries() {
	f.connectWg.Go(f.retryLoop)
	for i := range f.sentryClients {
		client := f.sentryClients[i]
		f.connectWg.Go(func() {
			f.receiveMessageLoop(client)
		})
		f.connectWg.Go(func() {
			f.receivePeerLoop(client)
		})
	}
}

func (f *Fetch) ConnectCore() {
	f.pool.MarkStateStale()
	f.connectWg.Go(func() {
		for {
			select {
			case <-f.ctx.Done():
				return
			default:
			}
			if err := f.handleStateChanges(f.ctx, f.stateChangesClient); err != nil {
				if errors.Is(err, errRefreshStateSnapshot) {
					continue
				}
				if !grpcutil.IsRetryLater(err) && !grpcutil.IsEndOfStream(err) {
					f.logger.Warn("[txpool.handleStateChanges]", "err", err)
				}
			}
			if common.Sleep(f.ctx, 3*time.Second) != nil {
				return
			}
		}
	})
}

// Wait blocks until all goroutines spawned by ConnectCore and ConnectSentries have exited.
func (f *Fetch) Wait() { f.connectWg.Wait() }

func (f *Fetch) receiveMessageLoop(sentryClient sentryproto.SentryClient) {
	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}
		if _, err := sentryClient.HandShake(f.ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				select {
				case <-f.ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				continue
			}
			// Report error and wait more
			f.logger.Warn("[txpool.recvMessage] sentry not ready yet", "err", err)
			continue
		}
		if err := f.receiveMessage(f.ctx, sentryClient); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				select {
				case <-f.ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				continue
			}
			f.logger.Warn("[txpool.recvMessage]", "err", err)
		}
	}
}

func (f *Fetch) receiveMessage(ctx context.Context, sentryClient sentryproto.SentryClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := sentryClient.Messages(streamCtx, &sentryproto.MessagesRequest{Ids: []sentryproto.MessageId{
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
		sentryproto.MessageId_TRANSACTIONS_66,
		sentryproto.MessageId_POOLED_TRANSACTIONS_66,
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
	}}, grpc.WaitForReady(true))
	if err != nil {
		select {
		case <-f.ctx.Done():
			return ctx.Err()
		default:
		}
		return err
	}

	const (
		maxBatchBytes    = 4 * 1024 * 1024
		maxBatchMessages = 256
	)
	var (
		batch      = make([]*sentryproto.InboundMessage, 0, maxBatchMessages)
		batchBytes int
		batchLock  sync.Mutex
		flushLock  sync.Mutex
	)

	// LRU cache for deduplication - filters duplicates before they enter the batch
	// Uses maphash to avoid string allocations
	seenLRU, _ := maphash.NewLRU[struct{}](1024)
	var seenNewBlocks uint64

	flushBatch := func() {
		flushLock.Lock()
		defer flushLock.Unlock()
		if streamCtx.Err() != nil {
			return
		}
		batchLock.Lock()
		if len(batch) == 0 {
			batchLock.Unlock()
			return
		}
		toProcess := batch
		batch = make([]*sentryproto.InboundMessage, 0, maxBatchMessages)
		batchBytes = 0
		batchLock.Unlock()

		if !f.pool.Started() {
			return
		}

		if f.db == nil {
			for range toProcess {
				if f.wg != nil {
					f.wg.Done()
				}
			}
			return
		}

		tx, txErr := f.db.BeginRo(streamCtx)
		if txErr != nil {
			f.logger.Warn("[txpool.fetch] failed to begin batch transaction", "err", txErr)
			return
		}
		defer tx.Rollback()

		for _, req := range toProcess {
			if handleErr := f.handleInboundMessageWithTx(streamCtx, tx, req, sentryClient); handleErr != nil {
				if !grpcutil.IsRetryLater(handleErr) && !grpcutil.IsEndOfStream(handleErr) {
					f.logger.Trace("[txpool.fetch] Handling batched message", "reqID", req.Id.String(), "err", handleErr)
				}
			}
			if f.wg != nil {
				f.wg.Done()
			}
		}
	}

	// Start ticker goroutine to flush batch.
	// Reduced from 1s to 250ms to lower worst-case latency between receiving
	// a POOLED_TRANSACTIONS_66 response and the transaction entering the pool.
	// This matters for block builders that need freshly-gossiped transactions
	// (e.g. multi-client blob tx ordering tests with a 2s payload window).
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-ticker.C:
				flushBatch()
			}
		}
	}()

	var req *sentryproto.InboundMessage
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			select {
			case <-f.ctx.Done():
				return ctx.Err()
			default:
			}
			return fmt.Errorf("txpool.receiveMessage: %w", err)
		}
		if req == nil {
			flushBatch()
			return nil
		}

		if req.Id == sentryproto.MessageId_POOLED_TRANSACTIONS_66 {
			if id, err := pooledTransactions66RequestID(req.Data); err == nil {
				f.releaseRetry(fetchPeer{sentryClient, gointerfaces.ConvertH512ToHash(req.PeerId)}, id)
			}
		}
		// Deduplicate transaction bodies to avoid repeated blob verification.
		// Keep announcements for peer fallback. A new block can make a rejected
		// transaction valid, so it starts a new deduplication scope.
		switch req.Id {
		case sentryproto.MessageId_TRANSACTIONS_66, sentryproto.MessageId_POOLED_TRANSACTIONS_66:
			if newBlocks := f.newBlocks.Load(); newBlocks != seenNewBlocks {
				seenLRU.Purge()
				seenNewBlocks = newBlocks
			}
			if _, seen := seenLRU.Get(req.Data); seen {
				if f.wg != nil {
					f.wg.Done()
				}
				continue
			}
			seenLRU.Set(req.Data, struct{}{})
		}

		batchLock.Lock()
		full := len(batch) >= maxBatchMessages || batchBytes+len(req.Data) > maxBatchBytes
		batchLock.Unlock()
		if full {
			flushBatch()
		}
		batchLock.Lock()
		batch = append(batch, req)
		batchBytes += len(req.Data)
		batchLock.Unlock()
	}
}

func (f *Fetch) handleInboundMessageWithTx(ctx context.Context, tx kv.Tx, req *sentryproto.InboundMessage, sentryClient sentryproto.SentryClient) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s, rlp: %x", rec, dbg.Stack(), req.Data)
		}
	}()

	if !f.pool.Started() {
		return nil
	}

	const maxHashesPerMsg = 4096 // See https://github.com/ethereum/devp2p/blob/master/caps/eth.md#newpooledtransactionhashes-0x08

	switch req.Id {
	case sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66:
		hashCount, pos, err := ParseHashesCount(req.Data, 0)
		if err != nil {
			f.logger.Debug("[txpool] penalizing peer for malformed NewPooledTransactionHashes66", "peer", req.PeerId, "err", err)
			f.penalizePeer(ctx, sentryClient, req.PeerId)
			return nil
		}

		if hashCount > maxHashesPerMsg {
			f.logger.Warn("Oversized hash announcement",
				"peer", req.PeerId, "count", hashCount)
			f.penalizePeer(ctx, sentryClient, req.PeerId)
			return nil
		}

		hashes := make([]byte, 32*hashCount)
		for i := 0; i < len(hashes); i += 32 {
			if _, pos, err = ParseHash(req.Data, pos, hashes[i:]); err != nil {
				f.logger.Debug("[txpool] penalizing peer for malformed NewPooledTransactionHashes66", "peer", req.PeerId, "err", err)
				f.penalizePeer(ctx, sentryClient, req.PeerId)
				return nil
			}
		}
		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}
		f.keepAnnouncersOfTracked(hashes, unknownHashes, sentryClient, req.PeerId)
		unknownHashes, reservedAt := f.skipRecentlyRequested(unknownHashes, sentryClient, req.PeerId)
		if err := f.requestUnknown(unknownHashes, reservedAt, fetchPeer{sentryClient, gointerfaces.ConvertH512ToHash(req.PeerId)}); err != nil {
			return err
		}
	case sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68:
		if count, err := peekAnnouncementCount(req.Data); err == nil && count > maxHashesPerMsg {
			f.logger.Warn("Oversized hash announcement",
				"peer", req.PeerId, "count", count)
			f.penalizePeer(ctx, sentryClient, req.PeerId)
			return nil
		}
		txTypes, sizes, hashes, _, err := parseAnnouncements(req.Data, 0)
		if err != nil {
			f.logger.Debug("[txpool] penalizing peer for malformed NewPooledTransactionHashes68", "peer", req.PeerId, "err", err)
			f.penalizePeer(ctx, sentryClient, req.PeerId)
			return nil
		}

		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}
		// Record only for hashes a reply can still come back for: unknown ones and tracked ones (a tracked one
		// the pool already holds stays until the next sweep forgets it).
		f.recordAnnouncement(req.PeerId, txTypes, sizes, hashes, f.keepAnnouncersOfTracked(hashes, unknownHashes, sentryClient, req.PeerId))
		unknownHashes, reservedAt := f.skipRecentlyRequested(unknownHashes, sentryClient, req.PeerId)

		if err := f.requestUnknown(unknownHashes, reservedAt, fetchPeer{sentryClient, gointerfaces.ConvertH512ToHash(req.PeerId)}); err != nil {
			return err
		}
	case sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:
		// TODO: handleInboundMessage is single-threaded - means it can accept as argument couple buffers (or analog of txParseContext). Protobuf encoding will copy data anyway, but DirectClient doesn't
		var encodedRequest []byte
		var messageID sentryproto.MessageId
		messageID = sentryproto.MessageId_POOLED_TRANSACTIONS_66
		requestID, hashes, _, err := ParseGetPooledTransactions66(req.Data, 0, nil)
		if err != nil {
			f.logger.Debug("[txpool] penalizing peer for malformed GetPooledTransactions66", "peer", req.PeerId, "err", err)
			f.penalizePeer(ctx, sentryClient, req.PeerId)
			return nil
		}

		// limit to max 256 transactions in a reply
		const hashSize = 32
		hashes = hashes[:min(len(hashes), 256*hashSize)]

		var txns [][]byte
		responseSize := 0
		processed := len(hashes)

		for i := 0; i < len(hashes); i += hashSize {
			if responseSize >= p2pTxPacketLimit {
				processed = i
				log.Trace("txpool.Fetch.handleInboundMessage PooledTransactions reply truncated to fit p2pTxPacketLimit", "requested", len(hashes), "processed", processed)
				break
			}

			txnHash := hashes[i:min(i+hashSize, len(hashes))]
			txn, err := f.pool.GetRlp(tx, txnHash)
			if err != nil {
				return err
			}
			if txn == nil {
				continue
			}

			txns = append(txns, txn)
			responseSize += len(txn)
		}

		encodedRequest = EncodePooledTransactions66(txns, requestID, nil)
		if len(encodedRequest) > p2pTxPacketLimit {
			log.Trace("txpool.Fetch.handleInboundMessage PooledTransactions reply exceeds p2pTxPacketLimit", "requested", len(hashes), "processed", processed)
		}

		if _, err := sentryClient.SendMessageById(f.ctx, &sentryproto.SendMessageByIdRequest{
			Data:   &sentryproto.OutboundMessageData{Id: messageID, Data: encodedRequest},
			PeerId: req.PeerId,
		}, &grpc.EmptyCallOption{}); err != nil {
			return err
		}
	case sentryproto.MessageId_POOLED_TRANSACTIONS_66, sentryproto.MessageId_TRANSACTIONS_66:
		txns := TxnSlots{}
		switch req.Id {
		case sentryproto.MessageId_TRANSACTIONS_66:
			if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
				if _, err := ParseTransactions(req.Data, 0, parseContext, &txns, func(hash []byte) error {
					known, err := f.pool.IdHashKnown(tx, hash)
					if err != nil {
						return fmt.Errorf("%w: %w", errInternalDB, err)
					}
					if known {
						return ErrRejected
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				if errors.Is(err, errInternalDB) {
					return err
				}
				f.logger.Debug("[txpool] penalizing peer for malformed Transactions66", "peer", req.PeerId, "err", err)
				f.penalizePeer(ctx, sentryClient, req.PeerId)
				return nil
			}
		case sentryproto.MessageId_POOLED_TRANSACTIONS_66:
			if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
				if _, _, err := ParsePooledTransactions66(req.Data, 0, parseContext, &txns, func(hash []byte) error {
					known, err := f.pool.IdHashKnown(tx, hash)
					if err != nil {
						return fmt.Errorf("%w: %w", errInternalDB, err)
					}
					if known {
						return ErrRejected
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				if errors.Is(err, errInternalDB) {
					return err
				}
				f.logger.Debug("[txpool] penalizing peer for malformed PooledTransactions66", "peer", req.PeerId, "err", err)
				f.penalizePeer(ctx, sentryClient, req.PeerId)
				return nil
			}
		default:
			return fmt.Errorf("unexpected message: %s", req.Id.String())
		}
		// Post-parse peer-behaviour checks. checkPooledTxnAnnouncement only
		// applies to the POOLED_TRANSACTIONS_66 (pull) path since TRANSACTIONS_66
		// has no prior announcement to compare against; checkBlobSidecar applies
		// to both because a peer can ship a blob-tx wrapper with mismatched
		// commitments via either message and must be kicked either way.
		for i := range txns.Txns {
			if req.Id == sentryproto.MessageId_POOLED_TRANSACTIONS_66 {
				if err := f.checkPooledTxnAnnouncement(req.PeerId, txns.Txns[i]); err != nil {
					f.logger.Debug("[txpool] penalizing peer for mismatched tx announcement", "peer", req.PeerId, "err", err)
					f.penalizePeer(ctx, sentryClient, req.PeerId)
					return nil
				}
			}
			if err := f.checkBlobSidecar(txns.Txns[i]); err != nil {
				f.logger.Debug("[txpool] penalizing peer for bad blob sidecar", "peer", req.PeerId, "err", err)
				f.penalizePeer(ctx, sentryClient, req.PeerId)
				return nil
			}
		}
		if len(txns.Txns) == 0 {
			return nil
		}
		f.pool.AddRemoteTxns(ctx, txns, req.PeerId, sentryClient)
	default:
		defer f.logger.Trace("[txpool] dropped p2p message", "id", req.Id)
	}

	return nil
}

func (f *Fetch) receivePeerLoop(sentryClient sentryproto.SentryClient) {
	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}
		if _, err := sentryClient.HandShake(f.ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				select {
				case <-f.ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				continue
			}
			// Report error and wait more
			f.logger.Warn("[txpool.recvPeers] sentry not ready yet", "err", err)
			select {
			case <-f.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if err := f.receivePeer(sentryClient); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				select {
				case <-f.ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				continue
			}

			f.logger.Warn("[txpool.recvPeers]", "err", err)
		}
	}
}

func (f *Fetch) receivePeer(sentryClient sentryproto.SentryClient) error {
	streamCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	stream, err := sentryClient.PeerEvents(streamCtx, &sentryproto.PeerEventsRequest{})
	if err != nil {
		select {
		case <-f.ctx.Done():
			return f.ctx.Err()
		default:
		}
		return err
	}

	var req *sentryproto.PeerEvent
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}
		if err := f.handleNewPeer(sentryClient, req); err != nil {
			return err
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleNewPeer(sentryClient sentryproto.SentryClient, req *sentryproto.PeerEvent) error {
	if req == nil {
		return nil
	}
	switch req.EventId {
	case sentryproto.PeerEvent_Connect:
		f.pool.AddNewGoodPeer(req.PeerId)
		f.requestedLock.Lock()
		delete(f.disconnected, fetchPeer{sentryClient, gointerfaces.ConvertH512ToHash(req.PeerId)})
		f.requestedLock.Unlock()
	case sentryproto.PeerEvent_Disconnect:
		f.dropAlternate(fetchPeer{sentryClient, gointerfaces.ConvertH512ToHash(req.PeerId)})
	}
	return nil
}

// dropAlternate removes a disconnected peer from the hashes it waits on as an alternate: fresh peer IDs would
// otherwise grow the alternates without bound.
func (f *Fetch) dropAlternate(peer fetchPeer) {
	f.requestedLock.Lock()
	defer f.requestedLock.Unlock()
	now := f.now()
	for p, at := range f.disconnected {
		if now.Sub(at) >= disconnectedPeerMemory {
			delete(f.disconnected, p)
		}
	}
	f.disconnected[peer] = now
	delete(f.retriesOutstanding, peer)
	f.waitingReady = true // a waiting request may have lost its last alternate
	for _, queue := range [][]pendingFetch{f.pending, f.waiting} {
		for _, p := range queue {
			if f.alternatesOf[peer] == 0 {
				return
			}
			if r, ok := f.requested.Get(p.hash[:]); ok && r.at.Equal(p.at) {
				if i := slices.Index(r.alternates, peer); i >= 0 {
					f.removeAlternate(r, i)
				}
			}
		}
	}
}

var errRefreshStateSnapshot = errors.New("refresh state snapshot")

func (f *Fetch) handleStateChanges(ctx context.Context, client StateChangesClient) error {
	// Any return ends this subscription, including a clean stream end. Keep the pool
	// stale until a new subscription restores a usable state view.
	defer f.pool.MarkStateStale()
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.StateChanges(streamCtx, &remoteproto.StateChangeRequest{WithStorage: true, WithTransactions: true}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	var refreshTimer *time.Timer
	for {
		batch, err := stream.Recv()
		if refreshTimer != nil {
			if !refreshTimer.Stop() {
				// The callback may still cancel the stream after Recv returns.
				// Keep transaction and activation observations, but recover state through a fresh subscription.
				if batch != nil {
					f.collectStateChangeTxns(batch)
					f.pool.RetainBogotaActivation(batch)
				}
				return errRefreshStateSnapshot
			}
			refreshTimer = nil
		}
		if err != nil {
			return err
		}
		if batch == nil {
			return nil
		}
		if err := f.handleStateChangesRequest(ctx, batch); err != nil {
			if !errors.Is(err, errStateViewSuperseded) {
				return err
			}
			// A newer commit may have no notification. Drain queued batches, but
			// reconnect for a fresh snapshot if the stream stays idle.
			refreshTimer = time.AfterFunc(time.Second, cancel)
		}

		if f.wg != nil { // to help tests
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleStateChangesRequest(ctx context.Context, req *remoteproto.StateChangeBatch) error {
	f.collectStateChangeTxns(req)
	for {
		applyErr := f.applyStateChanges(ctx, req)
		if applyErr == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if errors.Is(applyErr, errStateViewSuperseded) {
			return applyErr
		}
		if !errors.Is(applyErr, context.Canceled) {
			f.logger.Warn("[fetch] onNewBlock", "err", applyErr)
		}
		// Retry the failed batch: recovery must not depend on another block arriving.
		if err := common.Sleep(ctx, time.Second); err != nil {
			return err
		}
	}
}

func (f *Fetch) collectStateChangeTxns(req *remoteproto.StateChangeBatch) {
	changes := f.pendingTxnChanges
	// A transaction can be mined and unwound within one batch. Pool membership
	// follows its last direction, while state deltas must retain their full order.
	lastChange := make(map[[32]byte]int, len(changes))
	for i, change := range changes {
		lastChange[change.txn.IDHash] = i
	}
	for _, change := range req.ChangeBatch {
		if change.Direction != remoteproto.Direction_FORWARD && change.Direction != remoteproto.Direction_UNWIND {
			continue
		}
		for i, rlp := range change.Txs {
			parsed := txnChange{txn: &TxnSlot{}, direction: change.Direction}
			if err := f.threadSafeParseStateChangeTxn(func(parseContext *TxnParseContext) error {
				_, err := parseContext.ParseTransaction(rlp, 0, parsed.txn, parsed.sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
				return err
			}); err != nil {
				if !errors.Is(err, context.Canceled) {
					txnType, _ := PeekTransactionType(rlp)
					f.logger.Debug("[txpool.fetch] stream.Recv", "dir", change.Direction, "txnType", txnType, "index", i, "err", err)
				}
				continue // 1 txn handling error must not stop batch processing
			}
			lastChange[parsed.txn.IDHash] = len(changes)
			changes = append(changes, parsed)
		}
	}

	pending := changes[:0]
	for i, change := range changes {
		if lastChange[change.txn.IDHash] == i {
			pending = append(pending, change)
		}
	}
	clear(changes[len(pending):])
	f.pendingTxnChanges = pending
}

func (f *Fetch) applyStateChanges(ctx context.Context, req *remoteproto.StateChangeBatch) error {
	var unwindTxns, unwindBlobTxns, minedTxns TxnSlots
	for _, change := range f.pendingTxnChanges {
		// The pool mutates slot metadata, so each attempt needs its own copy.
		txn := *change.txn
		switch {
		case change.direction == remoteproto.Direction_FORWARD:
			minedTxns.Append(&txn, change.sender[:], false)
		case change.txn.Txn.GetBlobGas() != 0:
			unwindBlobTxns.Append(&txn, change.sender[:], false)
		default:
			unwindTxns.Append(&txn, change.sender[:], false)
		}
	}

	if err := f.pool.OnNewBlock(ctx, req, unwindTxns, unwindBlobTxns, minedTxns); err != nil {
		return err
	}
	f.pendingTxnChanges = nil
	f.newBlocks.Add(1)
	return nil
}
