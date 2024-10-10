// Trace types for sending proof information to a zk prover as defined in https://github.com/0xPolygonZero/proof-protocol-decoder.
package types

import (
	"github.com/holiman/uint256"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
)

type HexBytes []byte

func (b HexBytes) MarshalText() ([]byte, error) {
	return hexutility.Bytes(b[:]).MarshalText()
}

type ContractCodeUsage struct {
	Read  *libcommon.Hash `json:"read,omitempty"`
	Write HexBytes        `json:"write,omitempty"`
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
