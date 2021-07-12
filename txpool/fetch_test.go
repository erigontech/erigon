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
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

func TestFetch(t *testing.T) {
	mock := NewMockSentry()
	var genesisHash [32]byte
	var networkId uint64 = 1
	forks := []uint64{1, 5, 10}
	fetch := NewFetch(context.Background(), []sentry.SentryClient{mock}, genesisHash, networkId, forks)
	fmt.Printf("fetch: %+v\n", fetch)
}
