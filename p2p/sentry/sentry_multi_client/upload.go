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
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

const (
	maxConcurrentUploadRequests = 4
	maxQueuedUploadsPerPeer     = 4
)

type uploadTask struct {
	ctx     context.Context
	message *sentryproto.InboundMessage
	sentry  sentryproto.SentryClient
	done    func()
	size    int
}

type uploadPeer struct {
	active  bool
	pending []uploadTask
}

type uploadQueue struct {
	mu     sync.Mutex
	peers  map[[64]byte]*uploadPeer
	ready  [][64]byte
	active int
	queued int
	bytes  int
}

func (cs *MultiClient) enqueueUpload(task uploadTask) {
	peerID := gointerfaces.ConvertH512ToHash(task.message.PeerId)
	task.size = proto.Size(task.message)
	q := &cs.uploads
	q.mu.Lock()
	defer q.mu.Unlock()
	peer := q.peers[peerID]
	if task.ctx.Err() != nil || (peer != nil && len(peer.pending) >= maxQueuedUploadsPerPeer) {
		task.done()
		return
	}
	mustWait := q.active == maxConcurrentUploadRequests || (peer != nil && peer.active)
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
		peerID := q.ready[0]
		q.ready = q.ready[1:]
		peer := q.peers[peerID]
		task := peer.pending[0]
		peer.pending[0] = uploadTask{}
		peer.pending = peer.pending[1:]
		q.queued--
		q.bytes -= task.size
		peer.active = true
		q.active++
		go cs.runUpload(peerID, task)
	}
}

func (cs *MultiClient) runUpload(peerID [64]byte, task uploadTask) {
	defer task.done()
	defer func() {
		q := &cs.uploads
		q.mu.Lock()
		defer q.mu.Unlock()
		peer := q.peers[peerID]
		peer.active = false
		q.active--
		if len(peer.pending) > 0 {
			q.ready = append(q.ready, peerID)
		} else {
			delete(q.peers, peerID)
		}
		cs.startUploadsLocked()
	}()
	if task.ctx.Err() == nil {
		if err := cs.HandleInboundMessage(task.ctx, task.message, task.sentry); err != nil {
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
