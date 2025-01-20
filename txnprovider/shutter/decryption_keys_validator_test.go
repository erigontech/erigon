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
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
	shutterproto "github.com/erigontech/erigon/txnprovider/shutter/proto"
)

func TestDecryptionKeysValidator(t *testing.T) {
	t.Parallel()
	for _, tc := range decryptionKeysValidatorTestCases(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg, err := shutterproto.UnmarshallDecryptionKeys(tc.msg.Data)
			require.NoError(t, err)

			validator := shutter.NewDecryptionKeysValidator(shutter.Config{
				InstanceId:           testhelpers.TestInstanceId,
				MaxNumKeysPerMessage: testhelpers.TestMaxNumKeysPerMessage,
			})
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

			validator := shutter.NewDecryptionKeysP2pValidatorEx(logger, shutter.Config{
				InstanceId:           testhelpers.TestInstanceId,
				MaxNumKeysPerMessage: testhelpers.TestMaxNumKeysPerMessage,
			})
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
	wantErr               error
	wantValidationResult  pubsub.ValidationResult
	wantValidationLogMsgs []string
}

func decryptionKeysValidatorTestCases(t *testing.T) []decryptionKeysValidationTestCase {
	return []decryptionKeysValidationTestCase{
		{
			name: "instance id mismatch",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				InstanceIdOverride: 999999,
			}),
			wantErr:              shutter.ErrInstanceIdMismatch,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"instance id mismatch: 999999",
			},
		},
		{
			name: "missing gnosis extra data",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				NilExtra: true,
			}),
			wantErr:              shutter.ErrMissingGnosisExtraData,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"missing gnosis extra data",
			},
		},
		{
			name: "slot too large",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				Slot: math.MaxInt64 + 1,
			}),
			wantErr:              shutter.ErrSlotTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"slot too large: 9223372036854775808",
			},
		},
		{
			name: "tx pointer too large",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				TxPointer: math.MaxInt32 + 1,
			}),
			wantErr:              shutter.ErrTxPointerTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"tx pointer too large: 2147483648",
			},
		},
		{
			name: "eon too large",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				Eon: math.MaxInt64 + 1,
			}),
			wantErr:              shutter.ErrEonTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"eon too large: 9223372036854775808",
			},
		},
		{
			name: "empty keys",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				Keys:              [][]byte{},
				IdentityPreimages: [][]byte{},
			}),
			wantErr:              shutter.ErrEmptyKeys,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"empty keys",
			},
		},
		{
			name: "too many keys",
			msg: testhelpers.MockDecryptionKeysMsg(t, testhelpers.MockDecryptionKeysMsgOptions{
				Keys:              [][]byte{[]byte("key1"), []byte("key2"), []byte("key3"), []byte("key4")},
				IdentityPreimages: [][]byte{[]byte("id1"), []byte("id2"), []byte("id3"), []byte("id4")},
			}),
			wantErr:              shutter.ErrTooManyKeys,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"too many keys: 4",
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
