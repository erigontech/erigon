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

package rpc

import (
	"io"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
)

// The ingress budget bounds the request bytes a server holds at once, summed over HTTP
// bodies and websocket messages, which are otherwise only capped one at a time. The first
// smallBodyLimit bytes of a body are not charged: the request concurrency limit bounds
// those. The rest is charged as it arrives, never ahead of it, so holding budget costs a
// peer the same bytes.
const (
	smallBodyLimit       = int64(1 * datasize.MB)
	defaultIngressBudget = int64(256 * datasize.MB)
	// A budgeted read returns at most this much: the websocket reader fills the slice it is
	// given before it returns, so the charge would otherwise lag what a peer has sent by the
	// buffer's free capacity.
	maxBudgetedRead = 64 * datasize.KB
)

var errServerOverloaded = &CustomError{Code: ErrCodeServerOverloaded, Message: ErrMsgServerOverloaded}

type ingressBudget struct {
	limit int64
	used  atomic.Int64
}

func (b *ingressBudget) acquire(n int64) bool {
	if b.used.Add(n) > b.limit {
		b.used.Add(-n)
		ingressRejected.Inc()
		return false
	}
	return true
}

func (b *ingressBudget) release(n int64) {
	b.used.Add(-n)
}

// reader charges what is read from r to the budget, until release.
func (b *ingressBudget) reader(r io.Reader) *budgetedReader {
	return &budgetedReader{Reader: r, budget: b}
}

type budgetedReader struct {
	io.Reader
	budget  *ingressBudget
	read    int64
	charged int64
}

func (r *budgetedReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p[:min(len(p), int(maxBudgetedRead))])
	r.read += int64(n)
	if due := max(r.read-smallBodyLimit, 0) - r.charged; due > 0 {
		if !r.budget.acquire(due) {
			return n, errServerOverloaded
		}
		r.charged += due
	}
	return n, err
}

func (r *budgetedReader) release() {
	r.budget.release(r.charged)
}
