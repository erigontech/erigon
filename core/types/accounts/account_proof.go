package accounts

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
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

type SMTAccProofResult struct {
	Address         libcommon.Address       `json:"address"`
	Balance         *hexutil.Big            `json:"balance"`
	CodeHash        libcommon.Hash          `json:"codeHash"`
	CodeLength      hexutil.Uint64          `json:"codeLength"`
	Nonce           hexutil.Uint64          `json:"nonce"`
	BalanceProof    []hexutility.Bytes      `json:"balanceProof"`
	NonceProof      []hexutility.Bytes      `json:"nonceProof"`
	CodeHashProof   []hexutility.Bytes      `json:"codeHashProof"`
	CodeLengthProof []hexutility.Bytes      `json:"codeLengthProof"`
	StorageProof    []SMTStorageProofResult `json:"storageProof"`
}

type SMTStorageProofResult struct {
	Key   libcommon.Hash     `json:"key"`
	Value *hexutil.Big       `json:"value"`
	Proof []hexutility.Bytes `json:"proof"`
}
