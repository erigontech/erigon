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
	dkp.logger.Info("running decryption keys processor")

	//
	// TODO - the dkp can actually clean the encrypted and decrypted pools (so we wont have to use LRU)
	//      - uses block listener - when a new block comes in - calculate its slots using its timestamp (need changes to StateChange) - remove all decrypted txns for slots <= slot
	//      - remember previous txn pointer from the previous decryption keys msg - when the new one comes if the txn pointer has moved forward then drop all <= old tx pointer
	//        (this also raises the question - is it possible for the validator to reject old TxPointers:
	//          - i believe that once we verify key share identities that means we've received 100% reliable msgs from keypers
	//          - in that case, the validator can also keep track of "last validated keys txpointer" and reject those with txpointer < prev one
	//         )
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

	from := TxnIndex(msg.GetGnosis().TxPointer)
	to := from + TxnIndex(len(msg.Keys))
	encryptedTxns, err := dkp.encryptedTxnsPool.Txns(from, to, dkp.config.EncryptedGasLimit)
	if err != nil {
		return err
	}

	for _, encryptedTxn := range encryptedTxns {
		_, err := dkp.eonTracker.Eon(encryptedTxn.EonIndex)
		if err != nil {
			return err
		}

		//
		// TODO - decrypt txns using shcrypto
		//      - add them to the encrypted pool for the given msg.Slot
		//
	}

	return nil
}
