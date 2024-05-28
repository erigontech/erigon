package cltypes

import libcommon "github.com/ledgerwatch/erigon-lib/common"

type ValidatorRegistration struct {
	Message struct {
		FeeRecipient libcommon.Address `json:"fee_recipient"`
		GasLimit     string            `json:"gas_limit"`
		Timestamp    string            `json:"timestamp"`
		PubKey       libcommon.Bytes48 `json:"pubkey"`
	} `json:"message"`
	Signature libcommon.Bytes96 `json:"signature"`
}
