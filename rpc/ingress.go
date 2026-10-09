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
	"errors"
	"io"
	"sync/atomic"

	"github.com/c2h5oh/datasize"
)

// The ingress budget bounds the request bytes a server holds at once, summed over HTTP
// bodies and websocket messages, which are otherwise only capped one at a time. A body up
// to smallBodyLimit is not charged: the request concurrency limit bounds those.
const (
	smallBodyLimit       = int64(1 * datasize.MB)
	defaultIngressBudget = int64(256 * datasize.MB)
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

// admit charges what is read from r to the budget: a declared size before the first read,
// an undeclared one (declared < 0) the cap once it outgrows smallBodyLimit. A declared size
// that does not fit is refused before anything is allocated for it.
func (b *ingressBudget) admit(r io.Reader, declared int64) (*budgetedReader, bool) {
	br := &budgetedReader{Reader: r, budget: b}
	switch {
	case declared < 0:
		br.pending = maxRequestContentLength
	case declared > smallBodyLimit:
		if !b.acquire(declared) {
			return nil, false
		}
		br.charged = declared
	}
	return br, true
}

type budgetedReader struct {
	io.Reader
	budget  *ingressBudget
	read    int64
	charged int64
	pending int64 // charged once the body outgrows smallBodyLimit
}

func (r *budgetedReader) Read(p []byte) (int, error) {
	if r.pending > 0 {
		// At most one byte past smallBodyLimit is read before the charge, so a body that
		// ends within it is never charged.
		p = p[:min(int64(len(p)), smallBodyLimit+1-r.read)]
	}
	n, err := r.Reader.Read(p)
	r.read += int64(n)
	if r.pending > 0 && r.read > smallBodyLimit {
		if !r.budget.acquire(r.pending) {
			return n, errServerOverloaded
		}
		r.charged, r.pending = r.pending, 0
	}
	if errors.Is(err, io.EOF) && r.charged > r.read {
		// The body is complete, so a charge at the cap settles to the body's size.
		r.budget.release(r.charged - r.read)
		r.charged = r.read
	}
	return n, err
}

func (r *budgetedReader) release() {
	r.budget.release(r.charged)
}
