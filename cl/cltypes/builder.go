package cltypes

import libcommon "github.com/ledgerwatch/erigon-lib/common"

// ValidatorRegistration is used as request payload for validator registration in builder client.
type ValidatorRegistration struct {
	Message   ValidatorRegistrationMessage `json:"message"`
	Signature libcommon.Bytes96            `json:"signature"`
}

type ValidatorRegistrationMessage struct {
	FeeRecipient libcommon.Address `json:"fee_recipient"`
	GasLimit     string            `json:"gas_limit"`
	Timestamp    string            `json:"timestamp"`
	PubKey       libcommon.Bytes48 `json:"pubkey"`
}
