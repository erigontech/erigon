// Trace types for sending proof information to a zk prover as defined in https://github.com/0xPolygonZero/proof-protocol-decoder.
package types

import (
	"encoding/json"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

type HexBytes []byte

func (b HexBytes) MarshalText() ([]byte, error) {
	return hexutility.Bytes(b[:]).MarshalText()
}

type ContractCodeUsage struct {
	Read  *libcommon.Hash `json:"read,omitempty"`
	Write HexBytes        `json:"write,omitempty"`
}

type Uint256 struct {
	uint256.Int
}

// MarshalJSON implements the json.Marshaler interface
// It returns the hex representation of the uint256.Int.
// We need to do this because the uint256.Int will be marshalled in decimal format by default.
func (u *Uint256) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.Hex())
}

type TxnTrace struct {
	Balance        *uint256.Int                    `json:"balance,omitempty"`
	Nonce          *uint256.Int                    `json:"nonce,omitempty"`
	StorageRead    []libcommon.Hash                `json:"storage_read,omitempty"`
	StorageWritten map[libcommon.Hash]*uint256.Int `json:"storage_written,omitempty"`
	CodeUsage      *ContractCodeUsage              `json:"code_usage,omitempty"`
	SelfDestructed *bool                           `json:"self_destructed,omitempty"`
	StorageReadMap map[libcommon.Hash]struct{}     `json:"-"`
}

type TxnTraceHex struct {
	Balance        *Uint256                    `json:"balance,omitempty"`
	Nonce          *Uint256                    `json:"nonce,omitempty"`
	StorageRead    []libcommon.Hash            `json:"storage_read,omitempty"`
	StorageWritten map[libcommon.Hash]*Uint256 `json:"storage_written,omitempty"`
	CodeUsage      *ContractCodeUsage          `json:"code_usage,omitempty"`
	SelfDestructed *bool                       `json:"self_destructed,omitempty"`
	StorageReadMap map[libcommon.Hash]struct{} `json:"-"`
}

func (t *TxnTrace) MarshalJSON() ([]byte, error) {
	tHex := TxnTraceHex{
		StorageRead:    t.StorageRead,
		StorageWritten: make(map[libcommon.Hash]*Uint256),
		CodeUsage:      t.CodeUsage,
		SelfDestructed: t.SelfDestructed,
		StorageReadMap: t.StorageReadMap,
	}

	if t.Balance != nil {
		tHex.Balance = &Uint256{*t.Balance}
	}
	if t.Nonce != nil {
		tHex.Nonce = &Uint256{*t.Nonce}
	}

	for k, v := range t.StorageWritten {
		if v != nil {
			tHex.StorageWritten[k] = &Uint256{*v}
		}
	}

	return json.Marshal(tHex)
}

type TxnMeta struct {
	ByteCode           HexBytes `json:"byte_code,omitempty"`
	NewTxnTrieNode     HexBytes `json:"new_txn_trie_node_byte,omitempty"`
	NewReceiptTrieNode HexBytes `json:"new_receipt_trie_node_byte,omitempty"`
	GasUsed            uint64   `json:"gas_used,omitempty"`
}

type TxnInfo struct {
	Traces map[libcommon.Address]*TxnTrace `json:"traces,omitempty"`
	Meta   TxnMeta                         `json:"meta,omitempty"`
}

type BlockUsedCodeHashes []libcommon.Hash

type CombinedPreImages struct {
	Compact HexBytes `json:"compact,omitempty"`
}

type TriePreImage struct {
	Combined CombinedPreImages `json:"combined,omitempty"`
}

type BlockTrace struct {
	TriePreImage TriePreImage `json:"trie_pre_images,omitempty"`
	TxnInfo      []TxnInfo    `json:"txn_info,omitempty"`
}
