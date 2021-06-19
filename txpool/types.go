/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"fmt"

	"github.com/holiman/uint256"
)

// TxSlot contains information extracted from an Ethereum transaction, which is enough to manage it inside the transaction.
// Also, it contains some auxillary information, like ephemeral fields, and indices within priority queues
type TxSlot struct {
	txId        uint64      // Transaction id (distinct from transaction hash), used as a compact reference to a transaction accross data structures
	senderId    uint64      // Sender id (distinct from sender address), used as a compact referecne to to a sender accross data structures
	nonce       uint64      // Nonce of the transaction
	tip         uint64      // Maximum tip that transaction is giving to miner/block proposer
	feeCap      uint64      // Maximum fee that transaction burns and gives to the miner/block proposer
	gas         uint64      // Gas limit of the transaction
	value       uint256.Int // Value transferred by the transaction
	creation    bool        // Set to true if "To" field of the transation is not set
	dataLen     int         // Length of transaction's data (for calculation of intrinsic gas)
	alAddrCount int         // Number of addresses in the access list
	alStorCount int         // Number of storage keys in the access list
	bestIdx     int         // Index of the transaction in the best priority queue (of whatever pool it currently belongs to)
	worstIdx    int         // Index of the transaction in the worst priority queue (of whatever pook it currently belongs to)
	local       bool        // Whether transaction has been injected locally (and hence needs priority when mining or proposing a block)
}

// beInt parses Big Endian representation of an integer from given payload at given position
func beInt(payload []byte, pos, length int) (int, error) {
	var r int
	if length > 0 && payload[pos] == 0 {
		return 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[pos:pos+length])
	}
	for _, b := range payload[pos : pos+length] {
		r = (r << 8) | int(b)
	}
	return r, nil
}

// prefix parses RLP prefix from given payload at given position. It returns the offset and length of the RLP element
// as well as the indication of whether it is a list of string
func prefix(payload []byte, pos int) (dataPos int, dataLen int, list bool, err error) {
	switch first := payload[pos]; {
	case first < 128:
		dataPos = pos
		dataLen = 1
		list = false
	case first < 184:
		// string of len < 56, and it is non-legacy transaction
		dataPos = pos + 1
		dataLen = int(first) - 128
		list = false
	case first < 192:
		// string of len >= 56, and it is non-legacy transaction
		beLen := int(first) - 183
		dataPos = pos + 1 + beLen
		dataLen, err = beInt(payload, pos+1, beLen)
		list = false
	case first < 248:
		// list of len < 56, and it is a legacy transaction
		dataPos = pos + 1
		dataLen = int(first) - 192
		list = true
	default:
		// list of len >= 56, and it is a legacy transaction
		beLen := int(first) - 247
		dataPos = pos + 1 + beLen
		dataLen, err = beInt(payload, pos+1, beLen)
		list = true
	}
	return
}

// parseUint64 parses uint64 number from given payload at given position
func parseUint64(payload []byte, pos int) (int, uint64, error) {
	dataPos, dataLen, list, err := prefix(payload, pos)
	if err != nil {
		return 0, 0, err
	}
	if list {
		return 0, 0, fmt.Errorf("uint64 must be a string, not list")
	}
	if dataPos+dataLen >= len(payload) {
		return 0, 0, fmt.Errorf("unexpected end of payload")
	}
	if dataLen > 8 {
		return 0, 0, fmt.Errorf("uint64 must not be more than 8 bytes long, got %d", dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[dataPos:dataPos+dataLen])
	}
	var r uint64
	for _, b := range payload[dataPos : dataPos+dataLen] {
		r = (r << 8) | uint64(b)
	}
	return dataPos + dataLen, r, nil
}

// parseUint256 parses uint256 number from given payload at given position
func parseUint256(payload []byte, pos int, x *uint256.Int) (int, error) {
	dataPos, dataLen, list, err := prefix(payload, pos)
	if err != nil {
		return 0, err
	}
	if list {
		return 0, fmt.Errorf("uint256 must be a string, not list")
	}
	if dataPos+dataLen >= len(payload) {
		return 0, fmt.Errorf("unexpected end of payload")
	}
	if dataLen > 32 {
		return 0, fmt.Errorf("uint256 must not be more than 8 bytes long, got %d", dataLen)
	}
	if dataLen > 0 && payload[dataPos] == 0 {
		return 0, fmt.Errorf("integer encoding for RLP must not have leading zeros: %x", payload[dataPos:dataPos+dataLen])
	}
	x.SetBytes(payload[dataPos : dataPos+dataLen])
	return dataPos + dataLen, nil
}

const (
	LegacyTxType     int = 0
	AccessListTxType int = 1
	DynamicFeeTxType int = 2
)

// ParseTransaction extracts all the information from the transactions's payload (RLP) necessary to build TxSlot
// it also performs syntactic validation of the transactions
func ParseTransaction(payload []byte) (*TxSlot, error) {
	errorPrefix := "parse transaction payload"
	if len(payload) == 0 {
		return nil, fmt.Errorf("%s: empty rlp", errorPrefix)
	}
	// Legacy transations have list prefix, whereas EIP-2718 transactions have string prefix
	// therefore we assign the first returned value of prefix function (list) to legacy variable
	dataPos, dataLen, legacy, err := prefix(payload, 0)
	if err != nil {
		return nil, fmt.Errorf("%s: size prefix: %v", errorPrefix, err)
	}
	payloadLen := len(payload)
	if dataPos+dataLen >= payloadLen {
		return nil, fmt.Errorf("%s: unexpected end of payload after size prefix", errorPrefix)
	}
	pos := dataPos + dataLen

	var txType int
	// If it is non-legacy transaction, the transaction type follows
	if !legacy {
		txType = int(payload[pos])
		pos++
		if pos >= payloadLen {
			return nil, fmt.Errorf("%s: unexpected end of payload after txType", errorPrefix)
		}
	}
	// If it is non-legacy tx, chainId follows, but we skip it
	var list bool
	if !legacy {
		dataPos, dataLen, list, err = prefix(payload, pos)
		if err != nil {
			return nil, fmt.Errorf("%s: chainId len: %w", errorPrefix, err)
		}
		if list {
			return nil, fmt.Errorf("%s: chainId must be a string, not list", errorPrefix)
		}
		if dataPos+dataLen >= payloadLen {
			return nil, fmt.Errorf("%s: unexpected end of payload after chainId", errorPrefix)
		}
		pos = dataPos + dataLen
	}
	// Next follows the nonce, which we need to parse
	var slot TxSlot
	pos, slot.nonce, err = parseUint64(payload, pos)
	if err != nil {
		return nil, fmt.Errorf("%s: nonce: %w", errorPrefix, err)
	}
	// Next follows gas price or tip
	// Although consensus rules specify that tip can be up to 256 bit long, we narrow it to 64 bit
	pos, slot.tip, err = parseUint64(payload, pos)
	if err != nil {
		return nil, fmt.Errorf("%s: tip: %w", errorPrefix, err)
	}
	// Next follows feeCap, but only for dynamic fee transactions, for legacy transaction, it is
	// equal to tip
	if txType < DynamicFeeTxType {
		slot.feeCap = slot.tip
	} else {
		// Although consensus rules specify that feeCap can be up to 256 bit long, we narrow it to 64 bit
		pos, slot.feeCap, err = parseUint64(payload, pos)
		if err != nil {
			return nil, fmt.Errorf("%s: feeCap: %w", errorPrefix, err)
		}
	}
	// Next follows gas
	pos, slot.gas, err = parseUint64(payload, pos)
	if err != nil {
		return nil, fmt.Errorf("%s: gas: %w", errorPrefix, err)
	}
	// Next follows the destrination address (if present)
	dataPos, dataLen, list, err = prefix(payload, pos)
	if err != nil {
		return nil, fmt.Errorf("%s: to len: %w", errorPrefix, err)
	}
	if list {
		return nil, fmt.Errorf("%s: to must be a string, not list", errorPrefix)
	}
	if dataPos+dataLen >= payloadLen {
		return nil, fmt.Errorf("%s: unexpected end of payload after to", errorPrefix)
	}
	if dataLen != 0 && dataLen != 20 {
		return nil, fmt.Errorf("%s: unexpected length of to field: %d", errorPrefix, dataLen)
	}
	// Only note if To field is empty or not
	slot.creation = dataLen == 0
	pos = dataPos + dataLen
	// Next follows value
	pos, err = parseUint256(payload, pos, &slot.value)
	if err != nil {
		return nil, fmt.Errorf("%s: value: %w", errorPrefix, err)
	}
	// Next goes data, but we are only interesting in its length
	dataPos, dataLen, list, err = prefix(payload, pos)
	if err != nil {
		return nil, fmt.Errorf("%s: data len: %w", errorPrefix, err)
	}
	if list {
		return nil, fmt.Errorf("%s: data must be a string, not list", errorPrefix)
	}
	if dataPos+dataLen >= payloadLen {
		return nil, fmt.Errorf("%s: unexpected end of payload after data", errorPrefix)
	}
	slot.dataLen = dataLen
	pos = dataPos + dataLen
	// Next follows access list for non-legacy transactions, we are only interesting in number of addresses and storage keys
	dataPos, dataLen, list, err = prefix(payload, pos)
	if err != nil {
		return nil, fmt.Errorf("%s: access list len: %w", errorPrefix, err)
	}
	if !list {
		return nil, fmt.Errorf("%s: access list must be a list, not string", errorPrefix)
	}
	if dataPos+dataLen >= payloadLen {
		return nil, fmt.Errorf("%s: unexpected end of payload after access list", errorPrefix)
	}
	tuplePos := dataPos
	var tupleLen int
	for tuplePos < dataPos+dataLen {
		tuplePos, tupleLen, list, err = prefix(payload, tuplePos)
		if err != nil {
			return nil, fmt.Errorf("%s: tuple len: %w", errorPrefix, err)
		}
		if !list {
			return nil, fmt.Errorf("%s: tuple must be a list, not string", errorPrefix)
		}
		if tuplePos+tupleLen > dataPos+dataLen {
			return nil, fmt.Errorf("%s: unexpected end of access list after tuple", errorPrefix)
		}
		var addrPos, addrLen int
		addrPos, addrLen, list, err = prefix(payload, tuplePos)
		if err != nil {
			return nil, fmt.Errorf("%s: tuple addr len: %w", errorPrefix, err)
		}
		if list {
			return nil, fmt.Errorf("%s: tuple addr must be a string, not list", errorPrefix)
		}
		if addrPos+addrLen > tuplePos+tupleLen {
			return nil, fmt.Errorf("%s: unexpected end of tuple after address ", errorPrefix)
		}
		if addrLen != 20 {
			return nil, fmt.Errorf("%s: unexpected length of tuple address: %d", errorPrefix, addrLen)
		}
		slot.alAddrCount++
		skeyPos := addrPos + addrLen
		var skeyLen int
		for skeyPos < tuplePos+tupleLen {
			skeyPos, skeyLen, list, err = prefix(payload, tuplePos)
			if err != nil {
				return nil, fmt.Errorf("%s: tuple storage key len: %w", errorPrefix, err)
			}
			if list {
				return nil, fmt.Errorf("%s: tuple storage key must be a string, not list", errorPrefix)
			}
			if skeyPos+skeyPos > tuplePos+tuplePos {
				return nil, fmt.Errorf("%s: unexpected end of tuple after storage key", errorPrefix)
			}
			if skeyLen != 32 {
				return nil, fmt.Errorf("%s: unexpected length of tuple storage key: %d", errorPrefix, skeyLen)
			}
			slot.alStorCount++
			skeyPos = skeyPos + skeyLen
		}
		tuplePos = tuplePos + tupleLen
	}
	return &slot, nil
}
