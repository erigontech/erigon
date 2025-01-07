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

			validator := NewDecryptionKeysValidator(testInstanceId)
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
			logHandler := &collectingLogHandler{handler: logger.GetHandler()}
			logger.SetHandler(logHandler)

			validator := NewDecryptionKeysP2pValidatorEx(logger, testInstanceId)
			haveValidationResult := validator(ctx, "peer1", tc.msg)
			require.Equal(t, tc.wantValidationResult, haveValidationResult)
			require.True(t, logHandler.Contains(tc.wantValidationLogMsg))
		})
	}
}

const testInstanceId = 123

type decryptionKeysValidationTestCase struct {
	name                 string
	msg                  *pubsub.Message
	wantErr              error
	wantValidationResult pubsub.ValidationResult
	wantValidationLogMsg string
}

func decryptionKeysValidatorTestCases(t *testing.T) []decryptionKeysValidationTestCase {
	return []decryptionKeysValidationTestCase{
		instanceIdMismatchTestCase(t),
	}
}

func instanceIdMismatchTestCase(t *testing.T) decryptionKeysValidationTestCase {
	decryptionKeys, err := anypb.New(&shutterproto.DecryptionKeys{InstanceId: 999999})
	require.NoError(t, err)
	envelopeBytes, err := proto.Marshal(&shutterproto.Envelope{
		Version: shutterproto.EnvelopeVersion,
		Message: decryptionKeys,
	})
	require.NoError(t, err)
	topic := DecryptionKeysTopic
	return decryptionKeysValidationTestCase{
		name: "instance id mismatch",
		msg: &pubsub.Message{
			Message: &pb.Message{
				Data:  envelopeBytes,
				Topic: &topic,
			},
		},
		wantErr:              ErrInstanceIdMismatch,
		wantValidationResult: pubsub.ValidationReject,
		wantValidationLogMsg: "rejecting decryption keys msg due to data validation error err=\"instance id mismatch: 999999\"",
	}
}

func decryptionKeysP2pValidatorExTestCases(t *testing.T) []decryptionKeysValidationTestCase {
	tcs := decryptionKeysValidatorTestCases(t)
	tcs = append(tcs, invalidEnvelopeVersionTestCases(t))
	tcs = append(tcs, invalidMsgBytesTestCase())
	return tcs
}

func invalidEnvelopeVersionTestCases(t *testing.T) decryptionKeysValidationTestCase {
	decryptionKeys, err := anypb.New(&shutterproto.DecryptionKeys{InstanceId: testInstanceId})
	require.NoError(t, err)
	envelopeBytes, err := proto.Marshal(&shutterproto.Envelope{
		Version: "invalid envelope version",
		Message: decryptionKeys,
	})
	require.NoError(t, err)
	topic := DecryptionKeysTopic
	return decryptionKeysValidationTestCase{
		name: "invalid envelope version",
		msg: &pubsub.Message{
			Message: &pb.Message{
				Data:  envelopeBytes,
				Topic: &topic,
			},
		},
		wantValidationResult: pubsub.ValidationReject,
		wantValidationLogMsg: "rejecting decryption keys msg due to unmarshalling error err=\"envelope version mismatch: invalid envelope version\"",
	}
}

func invalidMsgBytesTestCase() decryptionKeysValidationTestCase {
	topic := DecryptionKeysTopic
	return decryptionKeysValidationTestCase{
		name: "invalid message bytes",
		msg: &pubsub.Message{
			Message: &pb.Message{
				Data:  []byte("invalid"),
				Topic: &topic,
			},
		},
		wantValidationResult: pubsub.ValidationReject,
		wantValidationLogMsg: "rejecting decryption keys msg due to unmarshalling error err=\"proto: cannot parse invalid wire-format data\"",
	}
}

var _ log.Handler = &collectingLogHandler{}

type collectingLogHandler struct {
	records []*log.Record
	handler log.Handler
}

func (c *collectingLogHandler) Log(r *log.Record) error {
	c.records = append(c.records, r)
	return c.handler.Log(r)
}

func (c *collectingLogHandler) Contains(subStr string) bool {
	for _, r := range c.records {
		msg := string(log.TerminalFormatNoColor().Format(r))
		if strings.Contains(msg, subStr) {
			return true
		}
	}
	return false
}
