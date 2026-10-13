// Copyright 2026 The Erigon Authors
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

package testfixtures

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// Live Chiado recordings in chiado/.
const (
	ChiadoAnchorFile        = "anchor-23507949.json"
	ChiadoRollupCreatedFile = "rollup-created-23413747.json"
)

// ChiadoSettlement is a settlement transaction mined on the Gnosis-hosted EEZ
// Chiado testnet, with its receipt logs and contract state read at the
// settling block.
type ChiadoSettlement struct {
	Source           string             `json:"source"`
	CapturedAt       string             `json:"capturedAt"`
	Network          string             `json:"network"`
	L1ChainID        uint64             `json:"l1ChainId"`
	L1BlockNumber    uint64             `json:"l1BlockNumber"`
	L1BlockHash      common.Hash        `json:"l1BlockHash"`
	L1BlockTimestamp uint64             `json:"l1BlockTimestamp"`
	Registry         common.Address     `json:"registry"`
	RollupManager    common.Address     `json:"rollupManager"`
	ProofSystem      common.Address     `json:"proofSystem"`
	ReceiptStatus    uint64             `json:"receiptStatus"`
	Transaction      ChiadoTransaction  `json:"transaction"`
	Logs             []ChiadoLog        `json:"logs"`
	StateAtBlock     ChiadoContractView `json:"stateAtBlock"`
}

// ChiadoTransaction is the settling EIP-4844 transaction as eth_getTransaction*
// returns it.
type ChiadoTransaction struct {
	Hash                 common.Hash         `json:"hash"`
	From                 common.Address      `json:"from"`
	TransactionIndex     hexutil.Uint64      `json:"transactionIndex"`
	Type                 hexutil.Uint64      `json:"type"`
	ChainID              hexutil.Big         `json:"chainId"`
	Nonce                hexutil.Uint64      `json:"nonce"`
	MaxPriorityFeePerGas hexutil.Big         `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         hexutil.Big         `json:"maxFeePerGas"`
	Gas                  hexutil.Uint64      `json:"gas"`
	To                   common.Address      `json:"to"`
	Value                hexutil.Big         `json:"value"`
	Input                hexutil.Bytes       `json:"input"`
	AccessList           []ChiadoAccessTuple `json:"accessList"`
	MaxFeePerBlobGas     hexutil.Big         `json:"maxFeePerBlobGas"`
	BlobVersionedHashes  []common.Hash       `json:"blobVersionedHashes"`
	YParity              hexutil.Uint64      `json:"yParity"`
	R                    hexutil.Big         `json:"r"`
	S                    hexutil.Big         `json:"s"`
}

// ChiadoAccessTuple is one access-list entry.
type ChiadoAccessTuple struct {
	Address     common.Address `json:"address"`
	StorageKeys []common.Hash  `json:"storageKeys"`
}

// ChiadoLog is one receipt log of the settling transaction.
type ChiadoLog struct {
	Address  common.Address `json:"address"`
	Topics   []common.Hash  `json:"topics"`
	Data     hexutil.Bytes  `json:"data"`
	LogIndex hexutil.Uint64 `json:"logIndex"`
}

// ChiadoContractView holds eth_call results at the settling block: the proof
// system's signer, the manager's key and threshold for it, and the registry's
// record for rollup 1.
type ChiadoContractView struct {
	ProofSystemSigner      common.Address `json:"proofSystemSigner"`
	ManagerVerificationKey common.Hash    `json:"managerVerificationKey"`
	ManagerThreshold       uint64         `json:"managerThreshold"`
	RollupContract         common.Address `json:"rollupContract"`
	RollupRoot             common.Hash    `json:"rollupRoot"`
	RollupEtherBalance     hexutil.Big    `json:"rollupEtherBalance"`
}

// LoadChiadoSettlement loads chiado/anchor-23507949.json.
func LoadChiadoSettlement() (*ChiadoSettlement, error) {
	var s ChiadoSettlement
	if err := readStrict(Path("chiado", ChiadoAnchorFile), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// ChiadoLogRecord is one receipt log read back from Chiado with the position
// of the transaction that emitted it.
type ChiadoLogRecord struct {
	Source           string         `json:"source"`
	CapturedAt       string         `json:"capturedAt"`
	Network          string         `json:"network"`
	L1ChainID        uint64         `json:"l1ChainId"`
	L1BlockNumber    uint64         `json:"l1BlockNumber"`
	L1BlockHash      common.Hash    `json:"l1BlockHash"`
	L1BlockTimestamp uint64         `json:"l1BlockTimestamp"`
	TransactionHash  common.Hash    `json:"transactionHash"`
	TransactionIndex hexutil.Uint64 `json:"transactionIndex"`
	Log              ChiadoLog      `json:"log"`
}

// LoadChiadoRollupCreated loads chiado/rollup-created-23413747.json, the
// registration of rollup 1 in the registry.
func LoadChiadoRollupCreated() (*ChiadoLogRecord, error) {
	var r ChiadoLogRecord
	if err := readStrict(Path("chiado", ChiadoRollupCreatedFile), &r); err != nil {
		return nil, err
	}
	return &r, nil
}
