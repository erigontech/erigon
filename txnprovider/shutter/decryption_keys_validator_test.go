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

package shutter_test

import (
	"context"
	"math"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shutterproto "github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func TestDecryptionKeysValidators(t *testing.T) {
	t.Parallel()

	instanceId := uint64(123)
	maxNumKeysPerMessage := uint64(10)
	config := shuttercfg.Config{
		InstanceId:           instanceId,
		MaxNumKeysPerMessage: maxNumKeysPerMessage,
	}
	eonIndex := shutter.EonIndex(76)
	threshold := uint64(2)
	numKeypers := uint64(3)
	ekg, err := testhelpers.MockEonKeyGeneration(eonIndex, threshold, numKeypers, 32123)
	require.NoError(t, err)
	eon := ekg.Eon()
	slot := uint64(6336)
	txnPointer := uint64(556)
	ips, err := testhelpers.MockIdentityPreimagesWithSlotIp(slot, 2)
	require.NoError(t, err)
	sigData := shutter.DecryptionKeysSignatureData{
		InstanceId:        instanceId,
		Eon:               eonIndex,
		Slot:              slot,
		TxnPointer:        txnPointer,
		IdentityPreimages: ips.ToListSSZ(),
	}
	signerIndices := []uint64{1, 2}
	signers := []testhelpers.Keyper{ekg.Keypers[signerIndices[0]], ekg.Keypers[signerIndices[1]]}
	keys, err := ekg.DecryptionKeys(signers, ips)
	require.NoError(t, err)
	sigs, err := testhelpers.Signatures(signers, sigData)
	require.NoError(t, err)
	maliciousSigners := []testhelpers.Keyper{ekg.Keypers[0], ekg.MaliciousKeyper}
	maliciousKeys, err := ekg.DecryptionKeys(maliciousSigners, ips)
	require.NoError(t, err)
	maliciousSigs, err := testhelpers.Signatures(maliciousSigners, sigData)
	require.NoError(t, err)

	for _, baseAndExtendedTc := range []decryptionKeysValidationTestCase{
		{
			name:   "instance id mismatch",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: 999999,
				}),
			),
			slotCalculator:        testhelpers.MockSlotCalculatorCreator(),
			eonTracker:            testhelpers.MockEonTrackerCreator(),
			wantErr:               shutter.ErrInstanceIdMismatch,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "instance id mismatch: 999999"},
		},
		{
			name:   "eon too large",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   math.MaxInt64 + 1,
				}),
			),
			slotCalculator:        testhelpers.MockSlotCalculatorCreator(),
			eonTracker:            testhelpers.MockEonTrackerCreator(),
			wantErr:               shutter.ErrEonTooLarge,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "eon too large: 9223372036854775808"},
		},
		{
			name:   "current eon unavailable",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: shutter.Eon{}, Ok: false}),
			),
			wantErr:               shutter.ErrCurrentEonUnavailable,
			wantValidationResult:  pubsub.ValidationIgnore,
			wantValidationLogMsgs: []string{"ignoring decryption keys msg due to", "current eon unavailable"},
		},
		{
			name:   "eon in the past",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   2,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: shutter.Eon{}, Ok: false}),
			),
			wantErr:              shutter.ErrEonInThePast,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"eon in the past: msgEonIndex=2, currentEonIndex=76",
			},
		},
		{
			name:   "eon in the future",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   156,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: shutter.Eon{}, Ok: false}),
			),
			wantErr:              shutter.ErrEonInTheFuture,
			wantValidationResult: pubsub.ValidationIgnore,
			wantValidationLogMsgs: []string{
				"ignoring decryption keys msg due to",
				"eon in the future: msgEonIndex=156, currentEonIndex=76",
			},
		},
		{
			name:   "eon not in recent",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{}),
			),
			wantErr:              shutter.ErrEonNotInRecent,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"eon not in recent: msgEonIndex=76, currentEonIndex=76",
			},
		},
		{
			name:   "empty keys",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
					Keys:       []*shutterproto.Key{},
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrEmptyKeys,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "empty keys"},
		},
		{
			name:   "too many keys",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
					Keys:       testhelpers.TestMustGenerateDecryptionKeys(t, ekg, signers, testhelpers.TestMustMockIdentityPreimages(t, maxNumKeysPerMessage+1)),
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrTooManyKeys,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "too many keys: 11"},
		},
		{
			name:   "missing gnosis extra data",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
					Keys:       keys,
					NilExtra:   true,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrMissingGnosisExtraData,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "missing gnosis extra data"},
		},
		{
			name:   "txn pointer too large",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
					Keys:       keys,
					TxnPointer: math.MaxInt32 + 1,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrTxnPointerTooLarge,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "txn pointer too large: 2147483648"},
		},
		{
			name:   "slot too large",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
					Keys:       keys,
					TxnPointer: txnPointer,
					Slot:       math.MaxInt64 + 1,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrSlotTooLarge,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "slot too large: 9223372036854775808"},
		},
		{
			name:   "slot in the past",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
					Keys:       keys,
					TxnPointer: txnPointer,
					Slot:       6334,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrSlotInThePast,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"slot in the past: msgSlot=6334, currentSlot=6336"},
		},
		{
			name:   "slot in the future",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:    shutterproto.EnvelopeVersion,
					InstanceId: instanceId,
					EonIndex:   eonIndex,
					Keys:       keys,
					TxnPointer: txnPointer,
					Slot:       6338,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrSlotInTheFuture,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"slot in the future: msgSlot=6338, currentSlot=6336",
			},
		},
		{
			name:   "signers threshold mismatch",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:       shutterproto.EnvelopeVersion,
					InstanceId:    instanceId,
					EonIndex:      eonIndex,
					Keys:          keys,
					TxnPointer:    txnPointer,
					Slot:          slot,
					SignerIndices: []uint64{1},
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrSignersThresholdMismatch,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "signers threshold mismatch: 1 != 2"},
		},
		{
			name:   "duplicate signers",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:       shutterproto.EnvelopeVersion,
					InstanceId:    instanceId,
					EonIndex:      eonIndex,
					Keys:          keys,
					TxnPointer:    txnPointer,
					Slot:          slot,
					SignerIndices: []uint64{1, 1},
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrDuplicateSigners,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "duplicate signers: 1"},
		},
		{
			name:   "unordered signers",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:       shutterproto.EnvelopeVersion,
					InstanceId:    instanceId,
					EonIndex:      eonIndex,
					Keys:          keys,
					TxnPointer:    txnPointer,
					Slot:          slot,
					SignerIndices: []uint64{2, 1},
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrUnorderedSigners,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "unordered signers: 1 < 2"},
		},
		{
			name:   "signer out of bounds",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:       shutterproto.EnvelopeVersion,
					InstanceId:    instanceId,
					EonIndex:      eonIndex,
					Keys:          keys,
					TxnPointer:    txnPointer,
					Slot:          slot,
					SignerIndices: []uint64{1, 777},
					Signatures:    sigs,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrInvalidKeyperIndex,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "invalid keyper index: 777 >= 3"},
		},
		{
			name:   "signatures len mismatch",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:       shutterproto.EnvelopeVersion,
					InstanceId:    instanceId,
					EonIndex:      eonIndex,
					Keys:          keys,
					TxnPointer:    txnPointer,
					Slot:          slot,
					SignerIndices: signerIndices,
					Signatures:    [][]byte{{'x'}},
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrSignaturesLenMismatch,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "signatures len mismatch: 1 != 2"},
		},
		{
			name:   "invalid signature",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:       shutterproto.EnvelopeVersion,
					InstanceId:    instanceId,
					EonIndex:      eonIndex,
					Keys:          keys,
					TxnPointer:    901, // diff txn pointer will result in diff sig
					Slot:          slot,
					SignerIndices: signerIndices,
					Signatures:    sigs,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrInvalidSignature,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"invalid signature: i=0, slot=6336, eon=76",
			},
		},
		{
			name:   "invalid key",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:       shutterproto.EnvelopeVersion,
					InstanceId:    instanceId,
					EonIndex:      eonIndex,
					Keys:          maliciousKeys,
					TxnPointer:    txnPointer,
					Slot:          slot,
					SignerIndices: signerIndices,
					Signatures:    maliciousSigs,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               shutter.ErrInvalidKey,
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to", "invalid key: ip=0x"},
		},
		{
			name:   "accept valid msg",
			config: config,
			msg: testhelpers.MockDecryptionKeysMsg(
				shutter.DecryptionKeysTopic,
				testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
					Version:       shutterproto.EnvelopeVersion,
					InstanceId:    instanceId,
					EonIndex:      eonIndex,
					Keys:          keys,
					TxnPointer:    txnPointer,
					Slot:          slot,
					SignerIndices: signerIndices,
					Signatures:    sigs,
				}),
			),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(testhelpers.WithCalcCurrentSlotMockResult(slot)),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:               nil,
			wantValidationResult:  pubsub.ValidationAccept,
			wantValidationLogMsgs: []string{},
		},
	} {
		tc := baseAndExtendedTc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Run("base", func(t *testing.T) {
				tc.testBaseValidator(t)
			})
			t.Run("extended", func(t *testing.T) {
				tc.testExtendedValidator(t)
			})
		})
	}

	for _, extendedOnlyTc := range []decryptionKeysValidationTestCase{
		{
			name:                 "unmarshalling error",
			config:               config,
			msg:                  testhelpers.MockDecryptionKeysMsg(shutter.DecryptionKeysTopic, []byte("invalid")),
			slotCalculator:       testhelpers.MockSlotCalculatorCreator(),
			eonTracker:           testhelpers.MockEonTrackerCreator(),
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to unmarshalling error",
				"cannot parse invalid wire-format data",
			},
		},
		{
			name:                  "topic mismatch",
			config:                config,
			msg:                   testhelpers.MockDecryptionKeysMsg("XXX", []byte("invalid")),
			slotCalculator:        testhelpers.MockSlotCalculatorCreator(),
			eonTracker:            testhelpers.MockEonTrackerCreator(),
			wantValidationResult:  pubsub.ValidationReject,
			wantValidationLogMsgs: []string{"rejecting decryption keys msg due to topic mismatch", "topic=XXX"},
		},
	} {
		tc := extendedOnlyTc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Run("extended", func(t *testing.T) {
				tc.testExtendedValidator(t)
			})
		})
	}
}

type decryptionKeysValidationTestCase struct {
	name                  string
	config                shuttercfg.Config
	msg                   *pubsub.Message
	slotCalculator        func(t *testing.T) *testhelpers.MockSlotCalculator
	eonTracker            func(t *testing.T) *testhelpers.MockEonTracker
	wantErr               error
	wantValidationResult  pubsub.ValidationResult
	wantValidationLogMsgs []string
}

func (tc decryptionKeysValidationTestCase) testBaseValidator(t *testing.T) {
	msg, err := shutterproto.UnmarshallDecryptionKeys(tc.msg.Data)
	require.NoError(t, err)

	validator := shutter.NewDecryptionKeysValidator(tc.config, tc.slotCalculator(t), tc.eonTracker(t))
	haveErr := validator.Validate(msg)
	require.ErrorIs(t, haveErr, tc.wantErr)
}

func (tc decryptionKeysValidationTestCase) testExtendedValidator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger := testlog.Logger(t, log.LvlCrit)
	logHandler := testhelpers.NewCollectingLogHandler(logger.GetHandler())
	logger.SetHandler(logHandler)

	validator := shutter.NewDecryptionKeysExtendedValidator(logger, tc.config, tc.slotCalculator(t), tc.eonTracker(t))
	haveValidationResult := validator(ctx, "peer1", tc.msg)
	require.Equal(t, tc.wantValidationResult, haveValidationResult)
	require.True(
		t,
		logHandler.ContainsAll(tc.wantValidationLogMsgs),
		"%v vs %v",
		tc.wantValidationLogMsgs,
		logHandler.FormattedRecords(),
	)
}
