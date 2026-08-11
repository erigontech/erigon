package services

import (
	"bytes"
	"errors"
)

type sszCanonicalEncoder interface {
	EncodeSSZ([]byte) ([]byte, error)
}

func requireCanonicalSSZ(input []byte, value sszCanonicalEncoder) error {
	encoded, err := value.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	if !bytes.Equal(input, encoded) {
		return errors.New("non-canonical SSZ encoding")
	}
	return nil
}
