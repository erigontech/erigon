package accounts

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"

	"github.com/ledgerwatch/erigon/common/hexutil"
)

// Result structs for GetProof
type AccProofResult struct {
	Address      libcommon.Address  `json:"address"`
	AccountProof []hexutility.Bytes `json:"accountProof"`
	Balance      *hexutil.Big       `json:"balance"`
	CodeHash     libcommon.Hash     `json:"codeHash"`
	Nonce        hexutil.Uint64     `json:"nonce"`
	StorageHash  libcommon.Hash     `json:"storageHash"`
	StorageProof []StorProofResult  `json:"storageProof"`
}
type StorProofResult struct {
	Key   libcommon.Hash     `json:"key"`
	Value *hexutil.Big       `json:"value"`
	Proof []hexutility.Bytes `json:"proof"`
}
