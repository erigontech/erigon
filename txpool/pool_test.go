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
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
)

func TestSenders(t *testing.T) {
	t.Run("evict_all_on_next_round", func(t *testing.T) {
		senders, require := newSendersCache(), require.New(t)
		_, tx := memdb.NewTestPoolTx(t)
		changed := roaring64.New()

		senders.senderIDs[fmt.Sprintf("%020x", 1)] = 1
		senders.senderInfo[1] = newSender(1, *uint256.NewInt(1))
		senders.senderIDs[fmt.Sprintf("%020x", 2)] = 2
		senders.senderInfo[2] = newSender(1, *uint256.NewInt(1))

		changed.AddMany([]uint64{1, 2})
		err := senders.flush(tx)
		require.NoError(err)

		changed.Clear()
		err = senders.flush(tx)
		require.NoError(err)
	})
	t.Run("evict_even_if_used_in_current_round_but_no_txs", func(t *testing.T) {
		senders, require := newSendersCache(), require.New(t)
		_, tx := memdb.NewTestPoolTx(t)

		senders.senderInfo[1] = newSender(1, *uint256.NewInt(1))
		senders.senderIDs[fmt.Sprintf("%020x", 1)] = 1
		senders.senderInfo[2] = newSender(1, *uint256.NewInt(1))
		senders.senderIDs[fmt.Sprintf("%020x", 2)] = 2

		changed := roaring64.New()
		changed.AddMany([]uint64{1, 2})
		err := senders.flush(tx)
		require.NoError(err)

		senders.senderInfo[1] = newSender(1, *uint256.NewInt(1)) // means used in current round, but still has 0 transactions
		senders.senderIDs[fmt.Sprintf("%020x", 1)] = 1
		changed.Clear()
		changed.AddMany([]uint64{1})
		err = senders.flush(tx)
		require.NoError(err)
	})

}
