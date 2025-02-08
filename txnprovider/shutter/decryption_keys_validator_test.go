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
	"github.com/erigontech/erigon/turbo/testlog"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shutterproto "github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
)

func TestDecryptionKeysValidator(t *testing.T) {
	t.Parallel()
	for _, tc := range decryptionKeysValidatorTestCases(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg, err := shutterproto.UnmarshallDecryptionKeys(tc.msg.Data)
			require.NoError(t, err)

			config := shutter.Config{
				InstanceId:           testhelpers.TestInstanceId,
				MaxNumKeysPerMessage: testhelpers.TestMaxNumKeysPerMessage,
			}
			validator := shutter.NewDecryptionKeysValidator(config, tc.slotCalculator(t), tc.eonTracker(t))
			haveErr := validator.Validate(msg)
			require.ErrorIs(t, haveErr, tc.wantErr)
		})
	}
}

func TestDecryptionKeysP2pValidatorEx(t *testing.T) {
	t.Parallel()
	for _, tc := range decryptionKeysP2pValidatorExTestCases(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			logger := testlog.Logger(t, log.LvlCrit)
			logHandler := testhelpers.NewCollectingLogHandler(logger.GetHandler())
			logger.SetHandler(logHandler)

			config := shutter.Config{
				InstanceId:           testhelpers.TestInstanceId,
				MaxNumKeysPerMessage: testhelpers.TestMaxNumKeysPerMessage,
			}
			validator := shutter.NewDecryptionKeysP2pValidatorEx(logger, config, tc.slotCalculator(t), tc.eonTracker(t))
			haveValidationResult := validator(ctx, "peer1", tc.msg)
			require.Equal(t, tc.wantValidationResult, haveValidationResult)
			require.True(
				t,
				logHandler.ContainsAll(tc.wantValidationLogMsgs),
				"%v vs %v",
				tc.wantValidationLogMsgs,
				logHandler.FormattedRecords(),
			)
		})
	}
}

type decryptionKeysValidationTestCase struct {
	name                  string
	msg                   *pubsub.Message
	slotCalculator        func(t *testing.T) shutter.SlotCalculator
	eonTracker            func(t *testing.T) shutter.EonTracker
	wantErr               error
	wantValidationResult  pubsub.ValidationResult
	wantValidationLogMsgs []string
}

func decryptionKeysValidatorTestCases(t *testing.T) []decryptionKeysValidationTestCase {
	ekg := testhelpers.MockEonKeyGeneration()
	eon := ekg.Eon()
	slot := uint64(6336)

	return []decryptionKeysValidationTestCase{
		{
			name: "instance id mismatch",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				InstanceIdOverride: 999999,
			}),
			slotCalculator:       testhelpers.MockSlotCalculatorCreator(),
			eonTracker:           testhelpers.MockEonTrackerCreator(),
			wantErr:              shutter.ErrInstanceIdMismatch,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"instance id mismatch: 999999",
			},
		},
		{
			name: "eon too large",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: math.MaxInt64 + 1,
			}),
			slotCalculator:       testhelpers.MockSlotCalculatorCreator(),
			eonTracker:           testhelpers.MockEonTrackerCreator(),
			wantErr:              shutter.ErrEonTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"eon too large: 9223372036854775808",
			},
		},
		{
			name: "current eon unavailable",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: 2,
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: shutter.Eon{}, Ok: false}),
			),
			wantErr:              shutter.ErrCurrentEonUnavailable,
			wantValidationResult: pubsub.ValidationIgnore,
			wantValidationLogMsgs: []string{
				"ignoring decryption keys msg due to",
				"current eon unavailable",
			},
		},
		{
			name: "eon in the past",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: 1,
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: shutter.Eon{Index: 2}, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: shutter.Eon{}, Ok: false}),
			),
			wantErr:              shutter.ErrEonInThePast,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"eon in the past: msgEonIndex=1, currentEonIndex=2",
			},
		},
		{
			name: "eon in the future",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: 3,
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: shutter.Eon{Index: 2}, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: shutter.Eon{}, Ok: false}),
			),
			wantErr:              shutter.ErrEonInTheFuture,
			wantValidationResult: pubsub.ValidationIgnore,
			wantValidationLogMsgs: []string{
				"ignoring decryption keys msg due to",
				"eon in the future: msgEonIndex=3, currentEonIndex=2",
			},
		},
		{
			name: "empty keys",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: eon.Index,
				Keys:     []*shutterproto.Key{},
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrEmptyKeys,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"empty keys",
			},
		},
		{
			name: "too many keys",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: eon.Index,
				Keys: ekg.DecryptionKeys(
					slot,
					// note 1 ip is appended by ekg.DecryptionKeys(slot, ips) so the below will breach the max
					testhelpers.MockIdentityPreimages(testhelpers.TestMaxNumKeysPerMessage)...,
				),
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrTooManyKeys,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"too many keys: 11",
			},
		},
		{
			name: "missing gnosis extra data",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: eon.Index,
				Keys:     ekg.DecryptionKeys(slot, testhelpers.MockIdentityPreimages(2)...),
				NilExtra: true,
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrMissingGnosisExtraData,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"missing gnosis extra data",
			},
		},
		{
			name: "txn pointer too large",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex:   eon.Index,
				Keys:       ekg.DecryptionKeys(slot, testhelpers.MockIdentityPreimages(2)...),
				TxnPointer: math.MaxInt32 + 1,
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrTxnPointerTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"txn pointer too large: 2147483648",
			},
		},
		{
			name: "slot too large",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: eon.Index,
				Keys:     ekg.DecryptionKeys(slot, testhelpers.MockIdentityPreimages(2)...),
				Slot:     math.MaxInt64 + 1,
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrSlotTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"slot too large: 9223372036854775808",
			},
		},
		{
			name: "slot in the past",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: eon.Index,
				Keys:     ekg.DecryptionKeys(slot, testhelpers.MockIdentityPreimages(2)...),
				Slot:     14,
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(
				testhelpers.WithCalcCurrentSlotMockResult(16),
			),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrSlotInThePast,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"slot in the past: msgSlot=14, currentSlot=16",
			},
		},
		{
			name: "slot in the future",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				EonIndex: eon.Index,
				Keys:     ekg.DecryptionKeys(slot, testhelpers.MockIdentityPreimages(2)...),
				Slot:     18,
			}),
			slotCalculator: testhelpers.MockSlotCalculatorCreator(
				testhelpers.WithCalcCurrentSlotMockResult(16),
			),
			eonTracker: testhelpers.MockEonTrackerCreator(
				testhelpers.WithCurrentEonMockResult(testhelpers.CurrentEonMockResult{Eon: eon, Ok: true}),
				testhelpers.WithRecentEonMockResult(testhelpers.RecentEonMockResult{Eon: eon, Ok: true}),
			),
			wantErr:              shutter.ErrSlotInTheFuture,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to",
				"slot in the future: msgSlot=18, currentSlot=16",
			},
		},
	}
}

func decryptionKeysP2pValidatorExTestCases(t *testing.T) []decryptionKeysValidationTestCase {
	return append(
		[]decryptionKeysValidationTestCase{
			{
				name: "unmarshalling error",
				msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
					EnvelopeDataOverride: []byte("invalid"),
				}),
				slotCalculator:       testhelpers.MockSlotCalculatorCreator(),
				eonTracker:           testhelpers.MockEonTrackerCreator(),
				wantValidationResult: pubsub.ValidationReject,
				wantValidationLogMsgs: []string{
					"rejecting decryption keys msg due to unmarshalling error",
					"cannot parse invalid wire-format data",
				},
			},
		},
		decryptionKeysValidatorTestCases(t)...,
	)
}
