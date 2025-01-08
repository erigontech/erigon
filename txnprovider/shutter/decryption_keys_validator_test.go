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
	"math"
	"strings"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/testlog"
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

			validator := NewDecryptionKeysValidator(Config{
				InstanceId:           TestInstanceId,
				MaxNumKeysPerMessage: TestMaxNumKeysPerMessage,
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
			logger := testlog.Logger(t, log.LvlDebug)
			logHandler := &CollectingLogHandler{handler: logger.GetHandler()}
			logger.SetHandler(logHandler)

			validator := NewDecryptionKeysP2pValidatorEx(logger, Config{
				InstanceId:           TestInstanceId,
				MaxNumKeysPerMessage: TestMaxNumKeysPerMessage,
			})
			haveValidationResult := validator(ctx, "peer1", tc.msg)
			require.Equal(t, tc.wantValidationResult, haveValidationResult)
			require.True(t, logHandler.ContainsAll(tc.wantValidationLogMsgs))
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
			msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
				InstanceIdOverride: 999999,
			}),
			wantErr:              ErrInstanceIdMismatch,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"instance id mismatch: 999999",
			},
		},
		{
			name: "missing gnosis extra data",
			msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
				NilExtra: true,
			}),
			wantErr:              ErrMissingGnosisExtraData,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"missing gnosis extra data",
			},
		},
		{
			name: "slot too large",
			msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
				Slot: math.MaxInt64 + 1,
			}),
			wantErr:              ErrSlotTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"slot too large: 9223372036854775808",
			},
		},
		{
			name: "tx pointer too large",
			msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
				TxPointer: math.MaxInt32 + 1,
			}),
			wantErr:              ErrTxPointerTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"tx pointer too large: 2147483648",
			},
		},
		{
			name: "eon too large",
			msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
				Eon: math.MaxInt64 + 1,
			}),
			wantErr:              ErrEonTooLarge,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"eon too large: 9223372036854775808",
			},
		},
		{
			name: "empty keys",
			msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
				Keys:              [][]byte{},
				IdentityPreimages: [][]byte{},
			}),
			wantErr:              ErrEmptyKeys,
			wantValidationResult: pubsub.ValidationReject,
			wantValidationLogMsgs: []string{
				"rejecting decryption keys msg due to data validation error",
				"empty keys",
			},
		},
		{
			name: "too many keys",
			msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
				Keys:              [][]byte{[]byte("key1"), []byte("key2"), []byte("key3"), []byte("key4")},
				IdentityPreimages: [][]byte{[]byte("id1"), []byte("id2"), []byte("id3"), []byte("id4")},
			}),
			wantErr:              ErrTooManyKeys,
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
				name: "invalid envelope version",
				msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
					VersionOverride: "XXX",
				}),
				wantErr:              shutterproto.ErrEnveloperVersionMismatch,
				wantValidationResult: pubsub.ValidationReject,
				wantValidationLogMsgs: []string{
					"rejecting decryption keys msg due to unmarshalling error",
					"envelope version mismatch: XXX",
				},
			},
			{
				name: "invalid message bytes",
				msg: MockDecryptionKeysMsg(t, DecryptionKeysMsgOptions{
					EnvelopeBytesOverride: []byte("invalid"),
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

const (
	TestInstanceId           = 123
	TestMaxNumKeysPerMessage = 3
)

type DecryptionKeysMsgOptions struct {
	Eon                   uint64
	Keys                  [][]byte
	IdentityPreimages     [][]byte
	NilExtra              bool
	Slot                  uint64
	TxPointer             uint64
	SignerIndices         []uint64
	Signatures            [][]byte
	InstanceIdOverride    uint64
	VersionOverride       string
	TopicOverride         string
	EnvelopeBytesOverride []byte
}

func MockDecryptionKeysMsg(t *testing.T, opts DecryptionKeysMsgOptions) *pubsub.Message {
	var instanceId uint64
	if opts.InstanceIdOverride != 0 {
		instanceId = opts.InstanceIdOverride
	} else {
		instanceId = TestInstanceId
	}

	decryptionKeys := &shutterproto.DecryptionKeys{
		InstanceId: instanceId,
		Eon:        opts.Eon,
	}

	require.Equal(t, len(opts.Keys), len(opts.IdentityPreimages))
	for i := range opts.Keys {
		decryptionKeys.Keys = append(decryptionKeys.Keys, &shutterproto.Key{
			Key:              opts.Keys[i],
			IdentityPreimage: opts.IdentityPreimages[i],
		})
	}

	if !opts.NilExtra {
		decryptionKeys.Extra = &shutterproto.DecryptionKeys_Gnosis{
			Gnosis: &shutterproto.GnosisDecryptionKeysExtra{
				Slot:          opts.Slot,
				TxPointer:     opts.TxPointer,
				SignerIndices: opts.SignerIndices,
				Signatures:    opts.Signatures,
			},
		}
	}

	var topic string
	if opts.TopicOverride != "" {
		topic = opts.TopicOverride
	} else {
		topic = DecryptionKeysTopic
	}

	var version string
	if opts.VersionOverride != "" {
		version = opts.VersionOverride
	} else {
		version = shutterproto.EnvelopeVersion
	}

	var data []byte
	if opts.EnvelopeBytesOverride != nil {
		data = opts.EnvelopeBytesOverride
	} else {
		decryptionKeysMsg, err := anypb.New(decryptionKeys)
		require.NoError(t, err)
		envelopeBytes, err := proto.Marshal(&shutterproto.Envelope{
			Version: version,
			Message: decryptionKeysMsg,
		})
		require.NoError(t, err)
		data = envelopeBytes
	}

	return &pubsub.Message{
		Message: &pb.Message{
			Data:  data,
			Topic: &topic,
		},
	}
}

type CollectingLogHandler struct {
	records []*log.Record
	handler log.Handler
}

func (clh *CollectingLogHandler) Log(r *log.Record) error {
	clh.records = append(clh.records, r)
	return clh.handler.Log(r)
}

func (clh *CollectingLogHandler) ContainsAll(subStrs []string) bool {
	for _, subStr := range subStrs {
		if !clh.Contains(subStr) {
			return false
		}
	}
	return true
}

func (clh *CollectingLogHandler) Contains(subStr string) bool {
	for _, r := range clh.records {
		msg := string(log.TerminalFormatNoColor().Format(r))
		if strings.Contains(msg, subStr) {
			return true
		}
	}
	return false
}
