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

package proto

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const EnvelopeVersion = "0.0.1"

var ErrEnveloperVersionMismatch = errors.New("envelope version mismatch")

func UnmarshallDecryptionKeys(envelopeBytes []byte) (*DecryptionKeys, error) {
	envelope, err := UnmarshallEnvelope(envelopeBytes)
	if err != nil {
		return nil, err
	}

	decryptionKeys := DecryptionKeys{}
	err = anypb.UnmarshalTo(envelope.Message, &decryptionKeys, proto.UnmarshalOptions{})
	if err != nil {
		return nil, err
	}

	return &decryptionKeys, nil
}

func UnmarshallEnvelope(envelopeBytes []byte) (*Envelope, error) {
	envelope := Envelope{}
	err := proto.Unmarshal(envelopeBytes, &envelope)
	if err != nil {
		return nil, err
	}

	if envelope.Version != EnvelopeVersion {
		return nil, fmt.Errorf("%w: %s", ErrEnveloperVersionMismatch, envelope.Version)
	}

	return &envelope, nil
}
