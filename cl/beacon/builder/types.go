package builder

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

type ExecutionPayloadHeader struct {
	Version string `json:"version"`
	Data    struct {
		Message   cltypes.Eth1Header `json:"message"`
		Signature string             `json:"signature"`
	} `json:"data"`
}

type ValidatorRegistration struct {
	Message struct {
		FeeRecipient string `json:"fee_recipient"`
		GasLimit     string `json:"gas_limit"`
		Timestamp    string `json:"timestamp"`
		PubKey       string `json:"pubkey"`
	} `json:"message"`
	Sginature string `json:"signature"`
}

type BlindedBlockResponse struct {
	Version string            `json:"version"`
	Data    cltypes.Eth1Block `json:"data"`
}
