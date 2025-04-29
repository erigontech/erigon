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

	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

var (
	ErrInstanceIdMismatch       = errors.New("instance id mismatch")
	ErrMissingGnosisExtraData   = errors.New("missing gnosis extra data")
	ErrSlotTooLarge             = errors.New("slot too large")
	ErrSlotInThePast            = errors.New("slot in the past")
	ErrSlotInTheFuture          = errors.New("slot in the future")
	ErrTxnPointerTooLarge       = errors.New("txn pointer too large")
	ErrEonTooLarge              = errors.New("eon too large")
	ErrCurrentEonUnavailable    = errors.New("current eon unavailable")
	ErrEonInThePast             = errors.New("eon in the past")
	ErrEonInTheFuture           = errors.New("eon in the future")
	ErrEonNotInRecent           = errors.New("eon not in recent")
	ErrEmptyKeys                = errors.New("empty keys")
	ErrTooManyKeys              = errors.New("too many keys")
	ErrIgnoreMsg                = errors.New("ignoring msg")
	ErrInvalidSignature         = errors.New("invalid signature")
	ErrInvalidKey               = errors.New("invalid key")
	ErrSignersThresholdMismatch = errors.New("signers threshold mismatch")
	ErrDuplicateSigners         = errors.New("duplicate signers")
	ErrUnorderedSigners         = errors.New("unordered signers")
	ErrSignaturesLenMismatch    = errors.New("signatures len mismatch")
)

type DecryptionKeysValidator struct {
	config         shuttercfg.Config
	slotCalculator SlotCalculator
	eonTracker     EonTracker
}

func NewDecryptionKeysValidator(config shuttercfg.Config, sc SlotCalculator, et EonTracker) DecryptionKeysValidator {
	return DecryptionKeysValidator{
		config:         config,
		slotCalculator: sc,
		eonTracker:     et,
	}
}

func (v DecryptionKeysValidator) Validate(msg *proto.DecryptionKeys) error {
	if msg.InstanceId != v.config.InstanceId {
		return fmt.Errorf("%w: %d != %d", ErrInstanceIdMismatch, msg.InstanceId, v.config.InstanceId)
	}

	eon, err := v.validateEonIndex(msg)
	if err != nil {
		return err
	}

	if err := v.validateKeys(msg, eon); err != nil {
		return err
	}

	return v.validateExtraData(msg, eon)
}

func (v DecryptionKeysValidator) validateExtraData(msg *proto.DecryptionKeys, eon Eon) error {
	extraData := msg.GetGnosis()
	if extraData == nil {
		return ErrMissingGnosisExtraData
	}

	if extraData.TxPointer > math.MaxInt32 {
		return fmt.Errorf("%w: %d", ErrTxnPointerTooLarge, extraData.TxPointer)
	}

	if err := v.validateSlot(msg); err != nil {
		return err
	}

	if err := v.validateSigners(msg, eon); err != nil {
		return err
	}

	return v.validateSignatures(msg, eon)
}

func (v DecryptionKeysValidator) validateSlot(msg *proto.DecryptionKeys) error {
	msgSlot := msg.GetGnosis().Slot
	if msgSlot > math.MaxInt64 {
		return fmt.Errorf("%w: %d", ErrSlotTooLarge, msgSlot)
	}

	// we are ok with a buffer of +/- 1 slot
	currentSlot := v.slotCalculator.CalcCurrentSlot()
	var slotBeforeCurrent uint64
	if currentSlot > 0 {
		slotBeforeCurrent = currentSlot - 1
	}

	if msgSlot < slotBeforeCurrent {
		return fmt.Errorf("%w: msgSlot=%d, currentSlot=%d", ErrSlotInThePast, msgSlot, currentSlot)
	}

	if msgSlot > currentSlot+1 {
		// msgSlot can be either for current slot or for the next one depending on timing, but not further ahead than that
		return fmt.Errorf("%w: msgSlot=%d, currentSlot=%d", ErrSlotInTheFuture, msgSlot, currentSlot)
	}

	return nil
}

func (v DecryptionKeysValidator) validateSigners(msg *proto.DecryptionKeys, eon Eon) error {
	extraData := msg.GetGnosis()
	if uint64(len(extraData.SignerIndices)) != eon.Threshold {
		return fmt.Errorf("%w: %d != %d", ErrSignersThresholdMismatch, len(extraData.SignerIndices), eon.Threshold)
	}

	for i := 1; i < len(extraData.SignerIndices); i++ {
		// note: check whether signerIndex is within >= 0 and < len(eon.Members) is covered by eon.KeyperAt(signerIndex)
		prev, curr := extraData.SignerIndices[i-1], extraData.SignerIndices[i]
		if curr == prev {
			return fmt.Errorf("%w: %d", ErrDuplicateSigners, curr)
		}
		if curr < prev {
			return fmt.Errorf("%w: %d < %d", ErrUnorderedSigners, curr, prev)
		}
	}

	return nil
}

func (v DecryptionKeysValidator) validateSignatures(msg *proto.DecryptionKeys, eon Eon) error {
	extraData := msg.GetGnosis()
	if len(extraData.Signatures) != len(extraData.SignerIndices) {
		return fmt.Errorf("%w: %d != %d", ErrSignaturesLenMismatch, len(extraData.Signatures), len(extraData.SignerIndices))
	}

	identityPreimages := make(IdentityPreimages, len(msg.Keys))
	for i, k := range msg.Keys {
		ip, err := IdentityPreimageFromBytes(k.IdentityPreimage)
		if err != nil {
			return err
		}

		identityPreimages[i] = ip
	}

	for i, sig := range extraData.Signatures {
		signerIdx := extraData.SignerIndices[i]
		signer, err := eon.KeyperAt(signerIdx)
		if err != nil {
			return err
		}

		signatureData := DecryptionKeysSignatureData{
			InstanceId:        msg.InstanceId,
			Eon:               EonIndex(msg.Eon),
			Slot:              extraData.Slot,
			TxnPointer:        extraData.TxPointer,
			IdentityPreimages: identityPreimages.ToListSSZ(),
		}

		ok, err := signatureData.Verify(sig, signer)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: i=%d, slot=%d, eon=%d", ErrInvalidSignature, i, extraData.Slot, msg.Eon)
		}
	}

	return nil
}

func (v DecryptionKeysValidator) validateKeys(msg *proto.DecryptionKeys, eon Eon) error {
	if len(msg.Keys) == 0 {
		return ErrEmptyKeys
	}

	if len(msg.Keys) > int(v.config.MaxNumKeysPerMessage) {
		return fmt.Errorf("%w: %d", ErrTooManyKeys, len(msg.Keys))
	}

	eonPublicKey, err := eon.PublicKey()
	if err != nil {
		return err
	}

	for _, key := range msg.Keys {
		epochSecretKey, err := EpochSecretKeyFromBytes(key.Key)
		if err != nil {
			return fmt.Errorf("issue converting eon secret key for identity: %w: %d", err, key.IdentityPreimage)
		}
		ok, err := crypto.VerifyEpochSecretKey(epochSecretKey, eonPublicKey, key.IdentityPreimage)
		if err != nil {
			return err
		}
		if ok {
			continue
		}

		// not valid
		fullErr := fmt.Errorf("verification of eon secret key failed: %w", ErrInvalidKey)
		ip, err := IdentityPreimageFromBytes(key.IdentityPreimage)
		if err != nil {
			return fmt.Errorf("%w: %w", fullErr, err)
		}

		return fmt.Errorf("%w: ip=%s", fullErr, ip)
	}

	return nil
}

func (v DecryptionKeysValidator) validateEonIndex(msg *proto.DecryptionKeys) (Eon, error) {
	if msg.Eon > math.MaxInt64 {
		return Eon{}, fmt.Errorf("%w: %d", ErrEonTooLarge, msg.Eon)
	}

	msgEonIndex := EonIndex(msg.Eon)
	currentEon, ok := v.eonTracker.CurrentEon()
	if !ok {
		// we're still syncing and are behind - ignore msg, without penalizing peer
		return Eon{}, fmt.Errorf("%w: %w: likely still syncing to tip", ErrIgnoreMsg, ErrCurrentEonUnavailable)
	}

	eon, inRecent := v.eonTracker.RecentEon(msgEonIndex)
	if msgEonIndex < currentEon.Index && !inRecent {
		return Eon{}, fmt.Errorf("%w: msgEonIndex=%d, currentEonIndex=%d", ErrEonInThePast, msgEonIndex, currentEon.Index)
	}
	if msgEonIndex > currentEon.Index && !inRecent {
		// we may be lagging behind - ignore msg, without penalizing peer
		return Eon{}, fmt.Errorf("%w: %w: msgEonIndex=%d, currentEonIndex=%d", ErrIgnoreMsg, ErrEonInTheFuture, msgEonIndex, currentEon.Index)
	}
	if !inRecent {
		// this should not ever happen because current eon should be always in recent, but guarding just in case of bugs
		return Eon{}, fmt.Errorf("%w: msgEonIndex=%d, currentEonIndex=%d", ErrEonNotInRecent, msgEonIndex, currentEon.Index)
	}

	return eon, nil
}

func NewDecryptionKeysExtendedValidator(logger log.Logger, config shuttercfg.Config, sc SlotCalculator, et EonTracker) pubsub.ValidatorEx {
	dkv := NewDecryptionKeysValidator(config, sc, et)
	validator := func(ctx context.Context, id peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
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

	// decorate the validator with metrics
	return func(ctx context.Context, id peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		result := validator(ctx, id, msg)
		if result == pubsub.ValidationReject {
			decryptionKeysRejections.Inc()
		}
		if result == pubsub.ValidationIgnore {
			decryptionKeysIgnores.Inc()
		}
		return result
	}
}
