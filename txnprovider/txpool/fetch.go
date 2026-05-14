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
	"sync"
	"time"

	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

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

// announceInfo records the (type, size) that a peer promised for a given hash
// in an eth/68 NewPooledTransactionHashes announcement. We keep the peer id so
// that a later PooledTransactions reply from a different peer does not clobber
// the check (two peers can legitimately announce the same hash).
type announceInfo struct {
	peerHash uint64 // maphash of the announcing peer's 64-byte id
	txnType  byte
	size     uint32
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
	wg                       *sync.WaitGroup // used for synchronisation in the tests (nil when not in tests)
	connectWg                sync.WaitGroup  // tracks goroutines spawned by ConnectCore/ConnectSentries
	stateChangesParseCtx     *TxnParseContext
	pooledTxnsParseCtx       *TxnParseContext
	sentryClients            []sentryproto.SentryClient // sentry clients that will be used for accessing the network
	stateChangesParseCtxLock sync.Mutex
	pooledTxnsParseCtxLock   sync.Mutex
	announcements            *maphash.LRU[announceInfo] // announceKey(txHash, peerHash) -> announced (type, size) from that peer
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
	f := &Fetch{
		ctx:                  ctx,
		sentryClients:        sentryClients,
		pool:                 pool,
		db:                   db,
		stateChangesClient:   stateChangesClient,
		stateChangesParseCtx: NewTxnParseContext(chainID).ChainIDRequired(), //TODO: change ctx if rules changed
		pooledTxnsParseCtx:   NewTxnParseContext(chainID).ChainIDRequired(),
		announcements:        announcements,
		wg:                   options.p2pFetcherWg,
		logger:               logger,
	}
	f.pooledTxnsParseCtx.ValidateRLP(f.pool.ValidateSerializedTxn)
	f.stateChangesParseCtx.ValidateRLP(f.pool.ValidateSerializedTxn)

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
	if slot == nil || slot.Txn == nil || slot.Txn.Type() != types.BlobTxType {
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
	for i := range f.sentryClients {
		f.connectWg.Add(2)
		go func(i int) {
			defer f.connectWg.Done()
			f.receiveMessageLoop(f.sentryClients[i])
		}(i)
		go func(i int) {
			defer f.connectWg.Done()
			f.receivePeerLoop(f.sentryClients[i])
		}(i)
	}
}

func (f *Fetch) ConnectCore() {
	f.connectWg.Add(1)
	go func() {
		defer f.connectWg.Done()
		for {
			select {
			case <-f.ctx.Done():
				return
			default:
			}
			if err := f.handleStateChanges(f.ctx, f.stateChangesClient); err != nil {
				if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
					select {
					case <-f.ctx.Done():
						return
					case <-time.After(3 * time.Second):
					}
					continue
				}
				f.logger.Warn("[txpool.handleStateChanges]", "err", err)
			}
		}
	}()
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

	var (
		batch     = make([]*sentryproto.InboundMessage, 0, 256)
		batchLock sync.Mutex
	)

	// LRU cache for deduplication - filters duplicates before they enter the batch
	// Uses maphash to avoid string allocations
	seenLRU, _ := maphash.NewLRU[struct{}](1024)

	flushBatch := func() {
		batchLock.Lock()
		if len(batch) == 0 {
			batchLock.Unlock()
			return
		}
		toProcess := batch
		batch = make([]*sentryproto.InboundMessage, 0, 256)
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

		// Skip duplicate tx-body messages (TRANSACTIONS_66, POOLED_TRANSACTIONS_66):
		// blob-tx verification is the expensive work worth amortizing. We must NOT
		// dedupe announcements (NEW_POOLED_TRANSACTION_HASHES_66/68) across peers —
		// when one peer delivers a bad tx for an announced hash and gets kicked,
		// the tx fetcher falls back to a different peer that announced the same
		// hash, which only works if each peer's announcement is actually processed.
		// Same-peer repeated announcements are not deduped here either: the
		// dominant cost per announcement (a FilterKnownIdHashes call plus one
		// GET_POOLED_TRANSACTIONS_66 per unknown hash) is self-limiting — once
		// the pool has ingested the tx, FilterKnownIdHashes returns empty for
		// all subsequent announcements. Per-peer announcement spam before that
		// is a peer-reputation concern, not a fetcher dedup one.
		switch req.Id {
		case sentryproto.MessageId_TRANSACTIONS_66, sentryproto.MessageId_POOLED_TRANSACTIONS_66:
			if _, seen := seenLRU.Get(req.Data); seen {
				if f.wg != nil {
					f.wg.Done()
				}
				continue
			}
			seenLRU.Set(req.Data, struct{}{})
		}

		batchLock.Lock()
		batch = append(batch, req)
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
			sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
			return nil
		}

		if hashCount > maxHashesPerMsg {
			f.logger.Warn("Oversized hash announcement",
				"peer", req.PeerId, "count", hashCount)
			sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick}) // Disconnect peer
			return nil
		}

		hashes := make([]byte, 32*hashCount)
		for i := 0; i < len(hashes); i += 32 {
			if _, pos, err = ParseHash(req.Data, pos, hashes[i:]); err != nil {
				f.logger.Debug("[txpool] penalizing peer for malformed NewPooledTransactionHashes66", "peer", req.PeerId, "err", err)
				sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
				return nil
			}
		}
		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}
		if len(unknownHashes) > 0 {
			var encodedRequest []byte
			var messageID sentryproto.MessageId
			if encodedRequest, err = EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
				return err
			}
			messageID = sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66
			if _, err = sentryClient.SendMessageById(f.ctx, &sentryproto.SendMessageByIdRequest{
				Data:   &sentryproto.OutboundMessageData{Id: messageID, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}
	case sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68:
		if count, err := peekAnnouncementCount(req.Data); err == nil && count > maxHashesPerMsg {
			f.logger.Warn("Oversized hash announcement",
				"peer", req.PeerId, "count", count)
			sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
			return nil
		}
		txTypes, sizes, hashes, _, err := parseAnnouncements(req.Data, 0)
		if err != nil {
			f.logger.Debug("[txpool] penalizing peer for malformed NewPooledTransactionHashes68", "peer", req.PeerId, "err", err)
			sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
			return nil
		}

		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}
		// Record only for unknown hashes — a POOLED_TRANSACTIONS_66 reply
		// won't come back for already-known txs, so entries for them would
		// just sit unused in the LRU until evicted.
		f.recordAnnouncement(req.PeerId, txTypes, sizes, hashes, unknownHashes)

		if len(unknownHashes) > 0 {
			var encodedRequest []byte
			var messageID sentryproto.MessageId
			if encodedRequest, err = EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
				return err
			}
			messageID = sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66
			if _, err = sentryClient.SendMessageById(f.ctx, &sentryproto.SendMessageByIdRequest{
				Data:   &sentryproto.OutboundMessageData{Id: messageID, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}
	case sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:
		//TODO: handleInboundMessage is single-threaded - means it can accept as argument couple buffers (or analog of txParseContext). Protobuf encoding will copy data anyway, but DirectClient doesn't
		var encodedRequest []byte
		var messageID sentryproto.MessageId
		messageID = sentryproto.MessageId_POOLED_TRANSACTIONS_66
		requestID, hashes, _, err := ParseGetPooledTransactions66(req.Data, 0, nil)
		if err != nil {
			f.logger.Debug("[txpool] penalizing peer for malformed GetPooledTransactions66", "peer", req.PeerId, "err", err)
			sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
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
				sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
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
				sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
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
					sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
					return nil
				}
			}
			if err := f.checkBlobSidecar(txns.Txns[i]); err != nil {
				f.logger.Debug("[txpool] penalizing peer for bad blob sidecar", "peer", req.PeerId, "err", err)
				sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick})
				return nil
			}
		}
		if len(txns.Txns) == 0 {
			return nil
		}

		f.pool.AddRemoteTxns(ctx, txns)
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
		if err = f.handleNewPeer(req); err != nil {
			return err
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleNewPeer(req *sentryproto.PeerEvent) error {
	if req == nil {
		return nil
	}
	switch req.EventId {
	case sentryproto.PeerEvent_Connect:
		f.pool.AddNewGoodPeer(req.PeerId)
	}

	return nil
}

func (f *Fetch) handleStateChanges(ctx context.Context, client StateChangesClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.StateChanges(streamCtx, &remoteproto.StateChangeRequest{WithStorage: false, WithTransactions: true}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for req, err := stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}
		if err := f.handleStateChangesRequest(ctx, req); err != nil {
			f.logger.Warn("[fetch] onNewBlock", "err", err)
		}

		if f.wg != nil { // to help tests
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleStateChangesRequest(ctx context.Context, req *remoteproto.StateChangeBatch) error {
	var unwindTxns, unwindBlobTxns, minedTxns TxnSlots
	for _, change := range req.ChangeBatch {
		if change.Direction == remoteproto.Direction_FORWARD {
			for i := range change.Txs {
				if err := f.threadSafeParseStateChangeTxn(func(parseContext *TxnParseContext) error {
					utx := &TxnSlot{}
					sender := make([]byte, 20)
					_, err := parseContext.ParseTransaction(change.Txs[i], 0, utx, sender, false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
					if err != nil {
						return err
					}
					minedTxns.Append(utx, sender, false)
					return nil
				}); err != nil && !errors.Is(err, context.Canceled) {
					txnType, _ := PeekTransactionType(change.Txs[i])
					f.logger.Debug("[txpool.fetch] stream.Recv", "dir", change.Direction, "txnType", txnType, "index", i, "err", err)
					continue // 1 txn handling error must not stop batch processing
				}

			}
		} else if change.Direction == remoteproto.Direction_UNWIND {
			for i := range change.Txs {
				if err := f.threadSafeParseStateChangeTxn(func(parseContext *TxnParseContext) error {
					utx := &TxnSlot{}
					sender := make([]byte, 20)
					_, err := parseContext.ParseTransaction(change.Txs[i], 0, utx, sender, false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
					if err != nil {
						return err
					}
					if utx.TxType() == BlobTxnType {
						unwindBlobTxns.Append(utx, sender, false)
					} else {
						unwindTxns.Append(utx, sender, false)
					}
					return nil
				}); err != nil && !errors.Is(err, context.Canceled) {
					txnType, _ := PeekTransactionType(change.Txs[i])
					f.logger.Debug("[txpool.fetch] stream.Recv", "dir", change.Direction, "txnType", txnType, "index", i, "err", err)
					continue // 1 txn handling error must not stop batch processing
				}
			}
		}
	}

	if err := f.pool.OnNewBlock(ctx, req, unwindTxns, unwindBlobTxns, minedTxns); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
