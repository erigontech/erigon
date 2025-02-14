// Copyright 2025 The Erigon Authors
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

package shutter

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

type DecryptionKeysProcessor struct {
	logger            log.Logger
	config            Config
	eonTracker        EonTracker
	encryptedTxnsPool EncryptedTxnsPool
	decryptedTxnsPool DecryptedTxnsPool
	queue             chan *proto.DecryptionKeys
}

func NewDecryptionKeysProcessor(logger log.Logger) DecryptionKeysProcessor {
	return DecryptionKeysProcessor{
		logger: logger,
		queue:  make(chan *proto.DecryptionKeys),
	}
}

func (dkp DecryptionKeysProcessor) Enqueue(msg *proto.DecryptionKeys) {
	dkp.queue <- msg
}

func (dkp DecryptionKeysProcessor) Run(ctx context.Context) error {
	defer dkp.logger.Info("decryption keys processor stopped")
	dkp.logger.Info("running decryption keys processor")

	//
	// TODO - the dkp can actually clean the encrypted and decrypted pools (so we wont have to use LRU)
	//      - uses block listener
	//         - for decrypted pool: when a new block comes in - calculate its slots using its timestamp (need changes to StateChange) - remove all decrypted txns for slots <= slot
	//         - for encrypted pool: remember previous eon+txn pointer from the previous decryption keys msg - when the new one comes if the eon+txn pointer have moved forward then drop all <= old tx pointer
	//      - to handle reorgs:
	//         - keep these in a doubly linked list of size 128 (for forking depth in blocks)
	//         - when len > 128 start cleaning up backwards until len <= 128
	//         - upon reorgs will need to
	//

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-dkp.queue:
			err := dkp.process(msg)
			if err != nil {
				dkp.logger.Error(
					"failed to process decryption keys message - skipping",
					"eon", msg.Eon,
					"slot", msg.GetGnosis().Slot,
					"err", err,
				)

				//
				// TODO - metrics for this?
				//
				continue
			}
		}
	}
}

func (dkp DecryptionKeysProcessor) process(msg *proto.DecryptionKeys) error {
	dkp.logger.Debug(
		"processing decryption keys message",
		"instanceId", msg.InstanceId,
		"eon", msg.Eon,
		"slot", msg.GetGnosis().Slot,
		"txPointer", msg.GetGnosis().TxPointer,
	)

	eonIndex := EonIndex(msg.Eon)
	from := TxnIndex(msg.GetGnosis().TxPointer)
	to := from + TxnIndex(len(msg.Keys))
	encryptedTxns, err := dkp.encryptedTxnsPool.Txns(eonIndex, from, to, dkp.config.EncryptedGasLimit)
	if err != nil {
		return err
	}

	for _, encryptedTxn := range encryptedTxns {
		_, ok := dkp.eonTracker.RecentEon(encryptedTxn.EonIndex)
		if !ok {
			return fmt.Errorf("eon not seen recently: %d", encryptedTxn.EonIndex)
		}

		//
		// TODO - decrypt txns using shcrypto
		//      - add them to the encrypted pool for the given msg.Slot
		//
	}

	return nil
}
