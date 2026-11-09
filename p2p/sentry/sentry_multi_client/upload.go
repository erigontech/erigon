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

	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

const (
	maxConcurrentUploadRequests = 4
	maxUploadRequestsPerPeer    = 5
)

var errReceiptCacheMiss = errors.New("receipt cache miss")

type uploadTask struct {
	ctx      context.Context
	message  *sentryproto.InboundMessage
	sentry   sentryproto.SentryClient
	done     func()
	size     int
	uncached bool
}

type uploadPeer struct {
	active  bool
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
			task.done()
			return
		}
	}
	mustWait := q.active == maxConcurrentUploadRequests || (peer != nil && (peer.active || len(peer.pending) > 0))
	if mustWait && (q.queued >= libsentry.MessagesQueueSize || q.bytes+task.size > libsentry.MessagesQueueByteLimit) {
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
		taskIndex := -1
		peerIndex := slices.IndexFunc(q.ready, func(peerID [64]byte) bool {
			taskIndex = slices.IndexFunc(q.peers[peerID].pending, func(task uploadTask) bool {
				return !task.uncached || !q.uncachedActive
			})
			return taskIndex >= 0
		})
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
		if task.uncached {
			q.uncachedActive = true
		}
		go cs.runUpload(peerID, task)
	}
}

func (cs *MultiClient) runUpload(peerID [64]byte, task uploadTask) {
	var retry bool
	defer func() {
		q := &cs.uploads
		q.mu.Lock()
		peer := q.peers[peerID]
		peer.active = false
		q.active--
		if task.uncached {
			q.uncachedActive = false
		}
		requeue := retry && task.ctx.Err() == nil && q.queued < libsentry.MessagesQueueSize && q.bytes+task.size <= libsentry.MessagesQueueByteLimit
		if requeue {
			task.uncached = true
			uncachedEnd := 0
			for uncachedEnd < len(peer.pending) && peer.pending[uncachedEnd].uncached {
				uncachedEnd++
			}
			peer.pending = slices.Insert(peer.pending, uncachedEnd, task)
			q.queued++
			q.bytes += task.size
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
			retry = true
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
	cs.startUploadsLocked()
}
