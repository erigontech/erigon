// Copyright 2026 The Erigon Authors
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

package sentry_multi_client

import (
	"context"
	"errors"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

const (
	maxConcurrentUploadRequests = 4
	maxUploadRequestsPerPeer    = 5
	// Leave room for other peers while allowing a maximum-size protocol request to wait.
	maxUploadQueuedBytesPerPeer = 16 * 1024 * 1024
)

var (
	errReceiptCacheMiss = errors.New("receipt cache miss")

	uploadDroppedByPeerCount  = metrics.GetOrCreateCounter(`p2p_sentry_upload_dropped_requests_total{reason="peer_count"}`)
	uploadDroppedByPeerBytes  = metrics.GetOrCreateCounter(`p2p_sentry_upload_dropped_requests_total{reason="peer_bytes"}`)
	uploadDroppedByQueueCount = metrics.GetOrCreateCounter(`p2p_sentry_upload_dropped_requests_total{reason="queue_count"}`)
	uploadDroppedByQueueBytes = metrics.GetOrCreateCounter(`p2p_sentry_upload_dropped_requests_total{reason="queue_bytes"}`)
)

type uploadTask struct {
	ctx     context.Context
	message *sentryproto.InboundMessage
	sentry  sentryproto.SentryClient
	done    func()
	size    int
	// While queued, uncached marks a known cache miss waiting for the replay slot.
	// While running, it means this task owns that slot, even if all receipts are cached.
	uncached bool
}

type uploadPeer struct {
	active bool
	// Known cache misses form a prefix in request order; new misses join its end.
	// Later bodies and cache probes may bypass that prefix while the replay slot is busy.
	pending []uploadTask
}

type uploadQueue struct {
	mu             sync.Mutex
	peers          map[[64]byte]*uploadPeer
	ready          [][64]byte
	active         int
	uncachedActive bool
	queued         int
	bytes          int
}

func (q *uploadQueue) canQueue(peer *uploadPeer, size int) bool {
	if q.queued >= libsentry.MessagesQueueSize {
		uploadDroppedByQueueCount.Inc()
		return false
	}
	if q.bytes+size > libsentry.MessagesQueueByteLimit {
		uploadDroppedByQueueBytes.Inc()
		return false
	}
	if peer != nil {
		for _, task := range peer.pending {
			size += task.size
		}
	}
	if size > maxUploadQueuedBytesPerPeer {
		uploadDroppedByPeerBytes.Inc()
		return false
	}
	return true
}

func (cs *MultiClient) enqueueUpload(task uploadTask) {
	peerID := gointerfaces.ConvertH512ToHash(task.message.PeerId)
	task.size = proto.Size(task.message)
	q := &cs.uploads
	q.mu.Lock()
	defer q.mu.Unlock()
	peer := q.peers[peerID]
	if task.ctx.Err() != nil {
		task.done()
		return
	}
	if peer != nil {
		requests := len(peer.pending)
		if peer.active {
			requests++
		}
		if requests >= maxUploadRequestsPerPeer {
			uploadDroppedByPeerCount.Inc()
			task.done()
			return
		}
	}
	mustWait := q.active == maxConcurrentUploadRequests || (peer != nil && peer.active)
	if mustWait && !q.canQueue(peer, task.size) {
		task.done()
		return
	}
	if peer == nil {
		peer = &uploadPeer{}
		q.peers[peerID] = peer
	}
	peer.pending = append(peer.pending, task)
	q.queued++
	q.bytes += task.size
	if !peer.active && len(peer.pending) == 1 {
		q.ready = append(q.ready, peerID)
	}
	cs.startUploadsLocked()
}

func (cs *MultiClient) startUploadsLocked() {
	q := &cs.uploads
	for q.active < maxConcurrentUploadRequests && len(q.ready) > 0 {
		peerIndex, taskIndex := -1, -1
		for i, peerID := range q.ready {
			taskIndex = slices.IndexFunc(q.peers[peerID].pending, func(task uploadTask) bool {
				return !task.uncached || !q.uncachedActive
			})
			if taskIndex >= 0 {
				peerIndex = i
				break
			}
		}
		if peerIndex < 0 {
			return
		}
		peerID := q.ready[peerIndex]
		q.ready = slices.Delete(q.ready, peerIndex, peerIndex+1)
		peer := q.peers[peerID]
		task := peer.pending[taskIndex]
		peer.pending = slices.Delete(peer.pending, taskIndex, taskIndex+1)
		q.queued--
		q.bytes -= task.size
		peer.active = true
		q.active++
		switch task.message.Id {
		case sentryproto.MessageId_GET_RECEIPTS_66, sentryproto.MessageId_GET_RECEIPTS_69, sentryproto.MessageId_GET_RECEIPTS_70:
			task.uncached = !q.uncachedActive
			q.uncachedActive = true
		}
		go cs.runUpload(peerID, task)
	}
}

func (cs *MultiClient) runUpload(peerID [64]byte, task uploadTask) {
	var cacheMiss bool
	defer func() {
		q := &cs.uploads
		q.mu.Lock()
		peer := q.peers[peerID]
		peer.active = false
		q.active--
		if task.uncached {
			q.uncachedActive = false
		}
		requeue := cacheMiss && task.ctx.Err() == nil
		if requeue {
			uncachedEnd := 0
			for uncachedEnd < len(peer.pending) && peer.pending[uncachedEnd].uncached {
				uncachedEnd++
			}
			// This handler frees a worker, but ready peers and earlier replays take precedence.
			mustWait := q.uncachedActive || len(q.ready) > 0 || uncachedEnd > 0
			requeue = !mustWait || q.canQueue(peer, task.size)
			if requeue {
				task.uncached = true
				peer.pending = slices.Insert(peer.pending, uncachedEnd, task)
				q.queued++
				q.bytes += task.size
			}
		}
		if len(peer.pending) > 0 {
			q.ready = append(q.ready, peerID)
		} else {
			delete(q.peers, peerID)
		}
		cs.startUploadsLocked()
		q.mu.Unlock()
		if !requeue {
			task.done()
		}
	}()
	if task.ctx.Err() == nil {
		err := cs.handleInboundMessage(task.ctx, task.message, task.sentry, !task.uncached)
		if errors.Is(err, errReceiptCacheMiss) {
			cacheMiss = true
		} else if err != nil {
			cs.logger.Debug("Handling incoming upload message", "err", err)
		}
	}
}

func (cs *MultiClient) cancelUploads(ctx context.Context) {
	q := &cs.uploads
	q.mu.Lock()
	defer q.mu.Unlock()
	for peerID, peer := range q.peers {
		peer.pending = slices.DeleteFunc(peer.pending, func(task uploadTask) bool {
			if task.ctx != ctx {
				return false
			}
			q.queued--
			q.bytes -= task.size
			task.done()
			return true
		})
		if !peer.active && len(peer.pending) == 0 {
			delete(q.peers, peerID)
		}
	}
	q.ready = slices.DeleteFunc(q.ready, func(peerID [64]byte) bool { return q.peers[peerID] == nil })
}
