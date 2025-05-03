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

type MockDecryptionKeysEnvelopeDataOptions struct {
	EonIndex      shutter.EonIndex
	Keys          []*shutterproto.Key
	NilExtra      bool
	Slot          uint64
	TxnPointer    uint64
	SignerIndices []uint64
	Signatures    [][]byte
	InstanceId    uint64
	Version       string
}

func TestMustMockDecryptionKeysEnvelopeData(t *testing.T, opts MockDecryptionKeysEnvelopeDataOptions) []byte {
	data, err := MockDecryptionKeysEnvelopeData(opts)
	require.NoError(t, err)
	return data
}

func MockDecryptionKeysEnvelopeData(opts MockDecryptionKeysEnvelopeDataOptions) ([]byte, error) {
	decryptionKeys := &shutterproto.DecryptionKeys{
		InstanceId: opts.InstanceId,
		Eon:        uint64(opts.EonIndex),
		Keys:       opts.Keys,
		Extra: &shutterproto.DecryptionKeys_Gnosis{
			Gnosis: &shutterproto.GnosisDecryptionKeysExtra{
				Slot:          opts.Slot,
				TxPointer:     opts.TxnPointer,
				SignerIndices: opts.SignerIndices,
				Signatures:    opts.Signatures,
			},
		},
	}

	if opts.NilExtra {
		decryptionKeys.Extra = nil
	}

	decryptionKeysMsg, err := anypb.New(decryptionKeys)
	if err != nil {
		return nil, err
	}

	envelopeData, err := proto.Marshal(&shutterproto.Envelope{
		Version: opts.Version,
		Message: decryptionKeysMsg,
	})
	if err != nil {
		return nil, err
	}

	return envelopeData, nil
}

func MockDecryptionKeysMsg(topic string, envelopeData []byte) *pubsub.Message {
	return &pubsub.Message{
		Message: &pb.Message{
			Topic: &topic,
			Data:  envelopeData,
		},
	}
}
