/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

// Fetch connects to sentry and impements eth/65 or eth/66 protocol regarding the transaction
// messages. It tries to "prime" the sentry with StatusData message containing given
// genesis hash and list of forks, but with zero max block and total difficulty
// Sentry should have a logic not to overwrite statusData with messages from tx pool
type Fetch struct {
	ctx           context.Context       // Context used for cancellation and closing of the fetcher
	sentryClients []sentry.SentryClient // sentry clients that will be used for accessing the network
	genesisHash   [32]byte              // Genesis hash of the network (for constructing StatusData message to sentry)
	networkId     uint64                // Network Id of the network (for constructing StatusData message to sentry)
	forks         []uint64              // list of forks that network went through (for constructing StatusData message to sentry)
	wg            *sync.WaitGroup       // Waitgroup used for synchronisation in the tests (nil when not in tests)
}

// NewFetch creates a new fetch object that will work with given sentry clients. Since the
// SentryClient here is an interface, it is suitable for mocking in tests (mock will need
// to implement all the functions of the SentryClient interface).
func NewFetch(ctx context.Context,
	sentryClients []sentry.SentryClient,
	genesisHash [32]byte,
	networkId uint64,
	forks []uint64,
) *Fetch {
	return &Fetch{
		ctx:           ctx,
		sentryClients: sentryClients,
		genesisHash:   genesisHash,
		networkId:     networkId,
		forks:         forks,
	}
}

func (f *Fetch) SetWaitGroup(wg *sync.WaitGroup) {
	f.wg = wg
}

// Start initialises connection to the sentry
func (f *Fetch) Start() {
	for i := range f.sentryClients {
		go func(i int) {
			f.receiveMessageLoop(f.sentryClients[i])
		}(i)
		go func(i int) {
			f.receivePeerLoop(f.sentryClients[i])
		}(i)
	}
}

func (f *Fetch) receiveMessageLoop(sentryClient sentry.SentryClient) {
	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

	}
}

func (f *Fetch) receivePeerLoop(sentryClient sentry.SentryClient) {

}
