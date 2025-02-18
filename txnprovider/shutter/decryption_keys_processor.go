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
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	shuttercrypto "github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

type DecryptionKeysProcessor struct {
	logger            log.Logger
	config            Config
	encryptedTxnsPool *EncryptedTxnsPool
	decryptedTxnsPool *DecryptedTxnsPool
	txnParseCtx       *txpool.TxnParseContext
	queue             chan *proto.DecryptionKeys
}

func NewDecryptionKeysProcessor(
	logger log.Logger,
	config Config,
	encryptedTxnsPool *EncryptedTxnsPool,
	decryptedTxnsPool *DecryptedTxnsPool,
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-dkp.queue:
			err := dkp.process(ctx, msg)
			if err != nil {
				return err
			}
		}
	}
}

func (dkp DecryptionKeysProcessor) process(ctx context.Context, msg *proto.DecryptionKeys) error {
	dkp.logger.Debug(
		"processing decryption keys message",
		"instanceId", msg.InstanceId,
		"eon", msg.Eon,
		"slot", msg.GetGnosis().Slot,
		"txPointer", msg.GetGnosis().TxPointer,
		"keys", len(msg.Keys),
	)

	keys := msg.Keys[1:] // skip placeholder (we can safely do this because msg has already been validated)
	eonIndex := EonIndex(msg.Eon)
	from := TxnIndex(msg.GetGnosis().TxPointer)
	to := from + TxnIndex(len(keys)) // [from,to)
	encryptedTxns, err := dkp.encryptedTxnsPool.Txns(eonIndex, from, to, dkp.config.EncryptedGasLimit)
	if err != nil {
		return err
	}

	txnIndexToKey := make(map[TxnIndex]*proto.Key, len(keys))
	for i, key := range keys {
		txnIndexToKey[from+TxnIndex(i)] = key
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(estimate.AlmostAllCPUs())
	txns := make([]types.Transaction, len(encryptedTxns))
	totalGasLimit := atomic.Uint64{}
	for i, encryptedTxn := range encryptedTxns {
		eg.Go(func() error {
			txn, err := dkp.decryptTxn(txnIndexToKey, encryptedTxn)
			if err != nil {
				dkp.logger.Warn(
					"failed to decrypt transaction - skipping",
					"slot", msg.GetGnosis().Slot,
					"eonIndex", msg.Eon,
					"txnIndex", encryptedTxn.TxnIndex,
					"err", err,
				)
				// we do not return err here since as per protocol we skip bad decryption
				// and also so that we don't interrupt other decryption goroutines
				return nil
			}

			txns[i] = txn
			totalGasLimit.Add(encryptedTxn.GasLimit.Uint64())
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return err
	}

	// txns that didn't pass decryption and other checks will be left nil -> filter those out
	filteredTxns := make([]types.Transaction, 0, len(txns))
	for _, txn := range txns {
		if txn == nil {
			continue
		}

		filteredTxns = append(filteredTxns, txn)
	}

	decryptionMark := DecryptionMark{Slot: msg.GetGnosis().Slot, Eon: eonIndex}
	txnBatch := TxnBatch{Transactions: filteredTxns, TotalGasLimit: totalGasLimit.Load()}
	dkp.decryptedTxnsPool.AddDecryptedTxns(decryptionMark, txnBatch)
	return nil
}

func (dkp DecryptionKeysProcessor) decryptTxn(keys map[TxnIndex]*proto.Key, sub EncryptedTxnSubmission) (types.Transaction, error) {
	key, ok := keys[sub.TxnIndex]
	if !ok {
		return nil, fmt.Errorf("key not found for txn index %d", sub.TxnIndex)
	}

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

	txn, err := types.DecodeWrappedTransaction(txnSlot.Rlp)
	if err != nil {
		return nil, fmt.Errorf("failed to decode transaction: %w", err)
	}

	if txn.Type() == types.BlobTxType {
		return nil, errors.New("blob txns not allowed in shutter")
	}

	if subGasLimit := sub.GasLimit.Uint64(); txn.GetGas() != subGasLimit {
		return nil, fmt.Errorf("txn gas limit mismatch: txn=%d, encryptedTxnSubmission=%d", txn.GetGas(), subGasLimit)
	}

	return txn, nil
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
