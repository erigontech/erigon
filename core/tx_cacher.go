// Copyright 2018 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package core

import (
	"sync"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/v3/common/debug"
	"github.com/erigontech/erigon/v3/core/types"
)

// txSenderCacherRequest is a request for recovering transaction senders with a
// specific signature scheme and caching it into the transactions themselves.
//
// The inc field defines the number of transactions to skip after each recovery,
// which is used to feed the same underlying input array to different threads but
// ensure they process the early transactions fast.
type txSenderCacherRequest struct {
	signer types.Signer
	txs    []types.Transaction
	inc    int
}

// TxSenderCacher is a helper structure to concurrently ecrecover transaction
// senders from digital signatures on background threads.
type TxSenderCacher struct {
	threads int
	tasks   chan *txSenderCacherRequest
	exitCh  chan struct{}
	wg      *sync.WaitGroup
}

// NewTxSenderCacher creates a new transaction sender background cacher and starts
// as many processing goroutines as allowed by the GOMAXPROCS on construction.
func NewTxSenderCacher(threads int) *TxSenderCacher {
	cacher := &TxSenderCacher{
		tasks:   make(chan *txSenderCacherRequest, threads),
		threads: threads,
		exitCh:  make(chan struct{}),
		wg:      &sync.WaitGroup{},
	}

	for i := 0; i < threads; i++ {
		cacher.wg.Add(1)
		go func() {
			defer debug.LogPanic()
			defer cacher.wg.Done()
			cacher.cache()
		}()
	}
	return cacher
}

// cache is an infinite loop, caching transaction senders from various forms of
// data structures.
func (cacher *TxSenderCacher) cache() {
	for task := range cacher.tasks {
		if err := libcommon.Stopped(cacher.exitCh); err != nil {
			return
		}

		for i := 0; i < len(task.txs); i += task.inc {
			task.txs[i].Sender(task.signer)
		}
	}
}

func (cacher *TxSenderCacher) Close() {
	libcommon.SafeClose(cacher.exitCh)
	close(cacher.tasks)
	cacher.wg.Wait()
}
