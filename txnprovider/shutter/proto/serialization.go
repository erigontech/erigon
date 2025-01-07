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
