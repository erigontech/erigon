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

package testhelpers

import (
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/erigontech/erigon/txnprovider/shutter"
	shutterproto "github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

const (
	TestInstanceId           = 123
	TestMaxNumKeysPerMessage = 3
)

type MockDecryptionKeysMsgOptions struct {
	Eon                  uint64
	Keys                 [][]byte
	IdentityPreimages    [][]byte
	NilExtra             bool
	Slot                 uint64
	TxPointer            uint64
	SignerIndices        []uint64
	Signatures           [][]byte
	InstanceIdOverride   uint64
	VersionOverride      string
	TopicOverride        string
	EnvelopeDataOverride []byte
}

func MockDecryptionKeysMsg(t *testing.T, opts MockDecryptionKeysMsgOptions) *pubsub.Message {
	var data []byte
	if opts.EnvelopeDataOverride != nil {
		data = opts.EnvelopeDataOverride
	} else {
		data = MockDecryptionKeysEnvelopeData(t, opts)
	}

	var topic string
	if opts.TopicOverride != "" {
		topic = opts.TopicOverride
	} else {
		topic = shutter.DecryptionKeysTopic
	}

	return &pubsub.Message{
		Message: &pb.Message{
			Data:  data,
			Topic: &topic,
		},
	}
}

func MockDecryptionKeysEnvelopeData(t *testing.T, opts MockDecryptionKeysMsgOptions) []byte {
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

	var version string
	if opts.VersionOverride != "" {
		version = opts.VersionOverride
	} else {
		version = shutterproto.EnvelopeVersion
	}

	decryptionKeysMsg, err := anypb.New(decryptionKeys)
	require.NoError(t, err)
	envelopeData, err := proto.Marshal(&shutterproto.Envelope{
		Version: version,
		Message: decryptionKeysMsg,
	})
	require.NoError(t, err)
	return envelopeData
}
