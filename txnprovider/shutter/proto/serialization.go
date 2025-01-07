package proto

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

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

	return &envelope, nil
}
