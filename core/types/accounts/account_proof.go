// Copyright 2024 The Erigon Authors
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

package accounts

import (
	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/erigon-lib/common/hexutility"
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
