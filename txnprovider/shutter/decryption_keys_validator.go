package shutter

import "github.com/erigontech/erigon/txnprovider/shutter/proto"

type DecryptionKeysValidator struct{}

func (v DecryptionKeysValidator) Validate(msg *proto.DecryptionKeys) error {
	// TODO - 2 validators 1 for the decryption keys msg and one wrapper around it for the libp2p validation checks
	return nil
}
