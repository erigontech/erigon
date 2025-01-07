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
	"errors"
	"fmt"
	"math"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter/proto"
)

var (
	ErrInstanceIdMismatch     = errors.New("instance id mismatch")
	ErrMissingGnosisExtraData = errors.New("missing gnosis extra data")
	ErrSlotTooLarge           = errors.New("slot too large")
	ErrTxPointerTooLarge      = errors.New("tx pointer too large")
	ErrEonTooLarge            = errors.New("eon too large")
	ErrEmptyKeys              = errors.New("empty keys")
)

type DecryptionKeysValidator struct {
	instanceId uint64
}

func NewDecryptionKeysValidator(instanceId uint64) DecryptionKeysValidator {
	return DecryptionKeysValidator{
		instanceId: instanceId,
	}
}

func (v DecryptionKeysValidator) Validate(msg *proto.DecryptionKeys) error {
	if msg.InstanceId != v.instanceId {
		return fmt.Errorf("%w: %d", ErrInstanceIdMismatch, msg.InstanceId)
	}

	gnosisExtraData := msg.GetGnosis()
	if gnosisExtraData == nil {
		return ErrMissingGnosisExtraData
	}

	if gnosisExtraData.Slot > math.MaxInt64 {
		return fmt.Errorf("%w: %d", ErrSlotTooLarge, gnosisExtraData.Slot)
	}

	if gnosisExtraData.TxPointer > math.MaxInt32 {
		return fmt.Errorf("%w: %d", ErrTxPointerTooLarge, gnosisExtraData.TxPointer)
	}

	if msg.Eon > math.MaxInt64 {
		return fmt.Errorf("%w: %d", ErrEonTooLarge, msg.Eon)
	}

	if len(msg.Keys) == 0 {
		return ErrEmptyKeys
	}

	//
	// TODO add more tests
	//

	//
	// TODO decide what to do about MaxNumKeysPerMessage (check with shutter team whether it is keyper/accessnode only specific or consumers should also validate on it)
	//if len(msg.Keys) > int(v.MaxNumKeysPerMessage) {
	//	return ErrTooManyKeys
	//}

	//
	// TODO add DecryptionKeys.Validate() equivalent which checks the Key unmarshalling into shcrypto.EpochSecretKey
	//

	//
	// TODO VerifyEpochSecretKey: check if we should be doing this validation
	//

	//
	// TODO add validation for signers and signatures based on keyper set and eon infos
	//
	return nil
}

func NewDecryptionKeysP2pValidatorEx(logger log.Logger, instanceId uint64) pubsub.ValidatorEx {
	dkv := NewDecryptionKeysValidator(instanceId)
	return func(ctx context.Context, id peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		if topic := msg.GetTopic(); topic != DecryptionKeysTopic {
			logger.Debug("rejecting decryption keys msg due to topic mismatch", "topic", topic, "peer", id)
			return pubsub.ValidationReject
		}

		decryptionKeys, err := proto.UnmarshallDecryptionKeys(msg.GetData())
		if err != nil {
			logger.Debug("rejecting decryption keys msg due to unmarshalling error", "err", err, "peer", id)
			return pubsub.ValidationReject
		}

		err = dkv.Validate(decryptionKeys)
		if err != nil {
			logger.Debug("rejecting decryption keys msg due to data validation error", "err", err, "peer", id)
			return pubsub.ValidationReject
		}

		return pubsub.ValidationAccept
	}
}
