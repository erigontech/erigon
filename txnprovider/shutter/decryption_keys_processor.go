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
	"bytes"
	"context"
	"errors"
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	shuttercrypto "github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

type DecryptionKeysProcessor struct {
	logger            log.Logger
	config            Config
	encryptedTxnsPool EncryptedTxnsPool
	decryptedTxnsPool DecryptedTxnsPool
	txnParseCtx       *txpool.TxnParseContext
	queue             chan *proto.DecryptionKeys
}

func NewDecryptionKeysProcessor(
	logger log.Logger,
	config Config,
	encryptedTxnsPool EncryptedTxnsPool,
	decryptedTxnsPool DecryptedTxnsPool,
) DecryptionKeysProcessor {
	return DecryptionKeysProcessor{
		logger:            logger,
		config:            config,
		encryptedTxnsPool: encryptedTxnsPool,
		decryptedTxnsPool: decryptedTxnsPool,
		txnParseCtx:       txpool.NewTxnParseContext(*config.ChainId).ChainIDRequired(),
		queue:             make(chan *proto.DecryptionKeys),
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
				return err
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
		"keys", len(msg.Keys),
	)

	eonIndex := EonIndex(msg.Eon)
	from := TxnIndex(msg.GetGnosis().TxPointer)
	to := from + TxnIndex(len(msg.Keys))
	encryptedTxns, err := dkp.encryptedTxnsPool.Txns(eonIndex, from, to, dkp.config.EncryptedGasLimit)
	if err != nil {
		return err
	}

	txns := make([]types.Transaction, len(msg.Keys))
	for i, encryptedTxn := range encryptedTxns {
		txn, err := dkp.decryptTxn(msg.Keys[i], encryptedTxn)
		if err != nil {
			dkp.logger.Warn(
				"failed to decrypt transaction - skipping",
				"slot", msg.GetGnosis().Slot,
				"eon", msg.Eon,
				"idx", from+TxnIndex(i),
				"err", err,
			)

			continue
		}

		txns[i] = txn
	}

	decryptionMark := DecryptionMark{
		Slot: msg.GetGnosis().Slot,
		Eon:  eonIndex,
	}

	dkp.decryptedTxnsPool.AddDecryptedTxns(decryptionMark, txns)
	return nil
}

func (dkp DecryptionKeysProcessor) decryptTxn(key *proto.Key, sub EncryptedTxnSubmission) (types.Transaction, error) {
	epochSecretKey, err := EpochSecretKeyFromBytes(key.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode epoch secret key during txn decryption: %w", err)
	}

	submissionIdentityPreimage := sub.IdentityPreimageBytes()
	if !bytes.Equal(key.IdentityPreimage, submissionIdentityPreimage) {
		return nil, identityPreimageMismatchErr(key.IdentityPreimage, submissionIdentityPreimage)
	}

	encryptedMessage := new(shuttercrypto.EncryptedMessage)
	err = encryptedMessage.Unmarshal(sub.EncryptedTransaction)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal encrypted message: %w", err)
	}

	decryptedMessage, err := encryptedMessage.Decrypt(epochSecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt message: %w", err)
	}

	var txnSlot txpool.TxnSlot
	var sender libcommon.Address
	_, err = dkp.txnParseCtx.ParseTransaction(decryptedMessage, 0, &txnSlot, sender[:], true, true, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse decrypted transaction: %w", err)
	}

	return types.DecodeWrappedTransaction(txnSlot.Rlp)
}

func identityPreimageMismatchErr(keyIpBytes, submissionIpBytes []byte) error {
	err := errors.New("identity preimage mismatch")

	keyIp, ipErr := IdentityPreimageFromBytes(keyIpBytes)
	if ipErr != nil {
		err = fmt.Errorf("%w: keyIp=%s", err, ipErr)
	} else {
		err = fmt.Errorf("%w: keyIp=%s", err, keyIp)
	}

	submissionIp, ipErr := IdentityPreimageFromBytes(submissionIpBytes)
	if ipErr != nil {
		err = fmt.Errorf("%w: submissionIp=%s", err, ipErr)
	} else {
		err = fmt.Errorf("%w: submissionIp=%s", err, submissionIp)
	}

	return err
}
