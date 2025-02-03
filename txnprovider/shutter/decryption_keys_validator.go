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

//go:build !abigen

package shutter

import (
	"context"
	"errors"
	"fmt"
	"math"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

var (
	ErrInstanceIdMismatch     = errors.New("instance id mismatch")
	ErrMissingGnosisExtraData = errors.New("missing gnosis extra data")
	ErrSlotTooLarge           = errors.New("slot too large")
	ErrSlotInThePast          = errors.New("slot in the past")
	ErrSlotInTheFuture        = errors.New("slot in the future")
	ErrTxPointerTooLarge      = errors.New("tx pointer too large")
	ErrEonTooLarge            = errors.New("eon too large")
	ErrCurrentEonUnavailable  = errors.New("current eon unavailable")
	ErrEonInThePast           = errors.New("eon in the past")
	ErrEonInTheFuture         = errors.New("eon in the future")
	ErrEmptyKeys              = errors.New("empty keys")
	ErrTooManyKeys            = errors.New("too many keys")
	ErrIgnoreMsg              = errors.New("ignoring msg")
)

type DecryptionKeysValidator struct {
	config         Config
	slotCalculator SlotCalculator
	eonTracker     EonTracker
}

func NewDecryptionKeysValidator(config Config, sc SlotCalculator, et EonTracker) DecryptionKeysValidator {
	return DecryptionKeysValidator{
		config:         config,
		slotCalculator: sc,
		eonTracker:     et,
	}
}

func (v DecryptionKeysValidator) Validate(msg *proto.DecryptionKeys) error {
	if msg.InstanceId != v.config.InstanceId {
		return fmt.Errorf("%w: %d", ErrInstanceIdMismatch, msg.InstanceId)
	}

	if err := v.validateExtraData(msg); err != nil {
		return err
	}

	if err := v.validateEonIndex(msg); err != nil {
		return err
	}

	return v.validateKeys(msg)
}

func (v DecryptionKeysValidator) validateExtraData(msg *proto.DecryptionKeys) error {
	gnosisExtraData := msg.GetGnosis()
	if gnosisExtraData == nil {
		return ErrMissingGnosisExtraData
	}

	if err := v.validateSlot(msg); err != nil {
		return err
	}

	if gnosisExtraData.TxPointer > math.MaxInt32 {
		return fmt.Errorf("%w: %d", ErrTxPointerTooLarge, gnosisExtraData.TxPointer)
	}

	return nil
}

func (v DecryptionKeysValidator) validateSlot(msg *proto.DecryptionKeys) error {
	msgSlot := msg.GetGnosis().Slot
	if msgSlot > math.MaxInt64 {
		return fmt.Errorf("%w: %d", ErrSlotTooLarge, msgSlot)
	}

	currentSlot := v.slotCalculator.CalcCurrentSlot()
	if msgSlot < currentSlot {
		return fmt.Errorf("%w: msgSlot=%d, currentSlot=%d", ErrSlotInThePast, msgSlot, currentSlot)
	}

	if msgSlot > currentSlot+1 {
		// msgSlot can be either for current slot or for the next one depending on timing, but not further ahead than that
		return fmt.Errorf("%w: msgSlot=%d, currentSlot=%d", ErrSlotInTheFuture, msgSlot, currentSlot)
	}

	return nil
}

func (v DecryptionKeysValidator) validateKeys(msg *proto.DecryptionKeys) error {
	if len(msg.Keys) == 0 {
		return ErrEmptyKeys
	}

	if len(msg.Keys) > int(v.config.MaxNumKeysPerMessage) {
		return fmt.Errorf("%w: %d", ErrTooManyKeys, len(msg.Keys))
	}

	//
	// TODO when we add the shcrypto library and smart contract state accessors:
	//      - add validation for signers and signatures based on keyper set and currentEon infos
	//      - add DecryptionKeys.Validate() equivalent which checks the Key unmarshalling into shcrypto.EpochSecretKey
	//      - add validation forVerifyEpochSecretKey: check if we should be doing this validation
	//

	return nil
}

func (v DecryptionKeysValidator) validateEonIndex(msg *proto.DecryptionKeys) error {
	if msg.Eon > math.MaxInt64 {
		return fmt.Errorf("%w: %d", ErrEonTooLarge, msg.Eon)
	}

	msgEonIndex := EonIndex(msg.Eon)
	currentEon, ok := v.eonTracker.CurrentEon()
	if !ok {
		// we're still syncing and are behind - ignore msg, without penalizing peer
		return fmt.Errorf("%w: %w", ErrIgnoreMsg, ErrCurrentEonUnavailable)
	}

	_, inRecent := v.eonTracker.RecentEon(msgEonIndex)
	if msgEonIndex < currentEon.Index && !inRecent {
		return fmt.Errorf("%w: msgEonIndex=%d, currentEonIndex=%d", ErrEonInThePast, msgEonIndex, currentEon.Index)
	}

	if msgEonIndex > currentEon.Index && !inRecent {
		// we may be lagging behind - ignore msg, without penalizing peer
		return fmt.Errorf("%w: %w: msgEonIndex=%d, currentEonIndex=%d", ErrIgnoreMsg, ErrEonInTheFuture, msgEonIndex, currentEon.Index)
	}

	return nil
}

func NewDecryptionKeysP2pValidatorEx(logger log.Logger, config Config, sc SlotCalculator, et EonTracker) pubsub.ValidatorEx {
	dkv := NewDecryptionKeysValidator(config, sc, et)
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
			if errors.Is(err, ErrIgnoreMsg) {
				logger.Debug("ignoring decryption keys msg due to", "err", err, "peer", id)
				return pubsub.ValidationIgnore
			}

			logger.Debug("rejecting decryption keys msg due to", "err", err, "peer", id)
			return pubsub.ValidationReject
		}

		return pubsub.ValidationAccept
	}
}
