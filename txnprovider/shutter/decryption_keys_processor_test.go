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

//go:build !abigen

package shutter

import (
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func TestCleanupIgnoresPreBeaconGenesisReset(t *testing.T) {
	for _, chainName := range []string{networkname.Gnosis, networkname.Chiado} {
		t.Run(chainName, func(t *testing.T) {
			config := shuttercfg.ConfigByChainName(chainName)
			decrypted := NewDecryptedTxnsPool()
			mark := DecryptionMark{Slot: 0}
			decrypted.AddDecryptedTxns(mark, TxnBatch{})
			processor := DecryptionKeysProcessor{
				logger:            log.New(),
				slotCalculator:    NewBeaconChainSlotCalculator(config.BeaconChainGenesisTimestamp, config.SecondsPerSlot),
				decryptedTxnsPool: decrypted,
				processed:         mapset.NewSet[ProcessedMark](),
			}
			require.NoError(t, processor.processBlockEventCleanup(BlockEvent{LatestBlockTime: 0}))
			_, present := decrypted.DecryptedTxns(mark)
			require.True(t, present, "a pre-genesis reset must not clean beacon slot zero")
		})
	}
}
