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
	"encoding/binary"
	"fmt"
	"hash"
	"math/bits"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/secp256k1"
	"golang.org/x/crypto/sha3"
)

type PeerID *types.H512

// TxContext is object that is required to parse transactions and turn transaction payload into TxSlot objects
// usage of TxContext helps avoid extra memory allocations
type TxParseContext struct {
	keccak1       hash.Hash
	keccak2       hash.Hash
	chainId, r, s uint256.Int // Signature values
	n27, n28, n35 uint256.Int
	buf           [65]byte // buffer needs to be enough for hashes (32 bytes) and for public key (65 bytes)
	sighash       [32]byte
	sig           [65]byte
	recCtx        *secp256k1.Context // Context for sender recovery
}

func NewTxParseContext() *TxParseContext {
	ctx := &TxParseContext{
		keccak1: sha3.NewLegacyKeccak256(),
		keccak2: sha3.NewLegacyKeccak256(),
		recCtx:  secp256k1.NewContext(),
	}
	ctx.n27.SetUint64(27)
	ctx.n28.SetUint64(28)
	ctx.n35.SetUint64(35)
	return ctx
}

// TxSlot contains information extracted from an Ethereum transaction, which is enough to manage it inside the transaction.
// Also, it contains some auxillary information, like ephemeral fields, and indices within priority queues
type TxSlot struct {
	//txId        uint64      // Transaction id (distinct from transaction hash), used as a compact reference to a transaction accross data structures
	//senderId    uint64      // Sender id (distinct from sender address), used as a compact referecne to to a sender accross data structures
	nonce       uint64      // Nonce of the transaction
	tip         uint64      // Maximum tip that transaction is giving to miner/block proposer
	feeCap      uint64      // Maximum fee that transaction burns and gives to the miner/block proposer
	gas         uint64      // Gas limit of the transaction
	value       uint256.Int // Value transferred by the transaction
	creation    bool        // Set to true if "To" field of the transation is not set
	dataLen     int         // Length of transaction's data (for calculation of intrinsic gas)
	alAddrCount int         // Number of addresses in the access list
	alStorCount int         // Number of storage keys in the access list
	//bestIdx     int         // Index of the transaction in the best priority queue (of whatever pool it currently belongs to)
	//worstIdx    int         // Index of the transaction in the worst priority queue (of whatever pook it currently belongs to)
	//local       bool        // Whether transaction has been injected locally (and hence needs priority when mining or proposing a block)
	idHash [32]byte // Transaction hash for the purposes of using it as a transaction Id
	sender [20]byte // Sender address for the transaction, recovered from the signature
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
	if dataPos+dataLen > len(payload) {
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

const ParseTransactionErrorPrefix = "parse transaction payload"

// ParseTransaction extracts all the information from the transactions's payload (RLP) necessary to build TxSlot
// it also performs syntactic validation of the transactions
func (ctx *TxParseContext) ParseTransaction(payload []byte, pos int) (*TxSlot, int, error) {
	if len(payload) == 0 {
		return nil, 0, fmt.Errorf("%s: empty rlp", ParseTransactionErrorPrefix)
	}
	// Compute transaction hash
	ctx.keccak1.Reset()
	ctx.keccak2.Reset()
	var slot TxSlot
	// Legacy transations have list prefix, whereas EIP-2718 transactions have string prefix
	// therefore we assign the first returned value of prefix function (list) to legacy variable
	dataPos, dataLen, legacy, err := prefix(payload, pos)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: size prefix: %v", ParseTransactionErrorPrefix, err)
	}
	payloadLen := len(payload)
	if dataPos+dataLen != payloadLen {
		return nil, 0, fmt.Errorf("%s: transaction must be either 1 list or 1 string", ParseTransactionErrorPrefix)
	}
	p := dataPos

	var txType int
	var list bool
	// If it is non-legacy transaction, the transaction type follows, and then the the list
	if !legacy {
		txType = int(payload[p])
		if _, err = ctx.keccak1.Write(payload[p : p+1]); err != nil {
			return nil, 0, fmt.Errorf("%s: computing idHash (hashing type prefix): %w", ParseTransactionErrorPrefix, err)
		}
		if _, err = ctx.keccak2.Write(payload[p : p+1]); err != nil {
			return nil, 0, fmt.Errorf("%s: computing signHash (hashing type prefix): %w", ParseTransactionErrorPrefix, err)
		}
		p++
		if p >= payloadLen {
			return nil, 0, fmt.Errorf("%s: unexpected end of payload after txType", ParseTransactionErrorPrefix)
		}
		dataPos, dataLen, list, err = prefix(payload, p)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: envelope prefix: %v", ParseTransactionErrorPrefix, err)
		}
		if !list {
			return nil, 0, fmt.Errorf("%s: envelope must be a list, not string", ParseTransactionErrorPrefix)
		}
		if dataPos+dataLen > payloadLen {
			return nil, 0, fmt.Errorf("%s: unexpected end of payload after envelope", ParseTransactionErrorPrefix)
		}
		// Hash the envelope, not the full payload
		if _, err = ctx.keccak1.Write(payload[p : dataPos+dataLen]); err != nil {
			return nil, 0, fmt.Errorf("%s: computing idHash (hashing the envelope): %w", ParseTransactionErrorPrefix, err)
		}
		p = dataPos
	}
	// Remember where signing hash data begins (it will need to be wrapped in an RLP list)
	sigHashPos := p
	// If it is non-legacy tx, chainId follows, but we skip it
	if !legacy {
		dataPos, dataLen, list, err = prefix(payload, p)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: chainId len: %w", ParseTransactionErrorPrefix, err)
		}
		if list {
			return nil, 0, fmt.Errorf("%s: chainId must be a string, not list", ParseTransactionErrorPrefix)
		}
		if dataPos+dataLen >= payloadLen {
			return nil, 0, fmt.Errorf("%s: unexpected end of payload after chainId", ParseTransactionErrorPrefix)
		}
		p = dataPos + dataLen
	}
	// Next follows the nonce, which we need to parse
	p, slot.nonce, err = parseUint64(payload, p)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: nonce: %w", ParseTransactionErrorPrefix, err)
	}
	// Next follows gas price or tip
	// Although consensus rules specify that tip can be up to 256 bit long, we narrow it to 64 bit
	p, slot.tip, err = parseUint64(payload, p)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: tip: %w", ParseTransactionErrorPrefix, err)
	}
	// Next follows feeCap, but only for dynamic fee transactions, for legacy transaction, it is
	// equal to tip
	if txType < DynamicFeeTxType {
		slot.feeCap = slot.tip
	} else {
		// Although consensus rules specify that feeCap can be up to 256 bit long, we narrow it to 64 bit
		p, slot.feeCap, err = parseUint64(payload, p)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: feeCap: %w", ParseTransactionErrorPrefix, err)
		}
	}
	// Next follows gas
	p, slot.gas, err = parseUint64(payload, p)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: gas: %w", ParseTransactionErrorPrefix, err)
	}
	// Next follows the destrination address (if present)
	dataPos, dataLen, list, err = prefix(payload, p)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: to len: %w", ParseTransactionErrorPrefix, err)
	}
	if list {
		return nil, 0, fmt.Errorf("%s: to must be a string, not list", ParseTransactionErrorPrefix)
	}
	if dataPos+dataLen >= payloadLen {
		return nil, 0, fmt.Errorf("%s: unexpected end of payload after to", ParseTransactionErrorPrefix)
	}
	if dataLen != 0 && dataLen != 20 {
		return nil, 0, fmt.Errorf("%s: unexpected length of to field: %d", ParseTransactionErrorPrefix, dataLen)
	}
	// Only note if To field is empty or not
	slot.creation = dataLen == 0
	p = dataPos + dataLen
	// Next follows value
	p, err = parseUint256(payload, p, &slot.value)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: value: %w", ParseTransactionErrorPrefix, err)
	}
	// Next goes data, but we are only interesting in its length
	dataPos, dataLen, list, err = prefix(payload, p)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: data len: %w", ParseTransactionErrorPrefix, err)
	}
	if list {
		return nil, 0, fmt.Errorf("%s: data must be a string, not list", ParseTransactionErrorPrefix)
	}
	if dataPos+dataLen >= payloadLen {
		return nil, 0, fmt.Errorf("%s: unexpected end of payload after data", ParseTransactionErrorPrefix)
	}
	slot.dataLen = dataLen
	p = dataPos + dataLen
	// Next follows access list for non-legacy transactions, we are only interesting in number of addresses and storage keys
	if !legacy {
		dataPos, dataLen, list, err = prefix(payload, p)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: access list len: %w", ParseTransactionErrorPrefix, err)
		}
		if !list {
			return nil, 0, fmt.Errorf("%s: access list must be a list, not string", ParseTransactionErrorPrefix)
		}
		if dataPos+dataLen >= payloadLen {
			return nil, 0, fmt.Errorf("%s: unexpected end of payload after access list", ParseTransactionErrorPrefix)
		}
		tuplePos := dataPos
		var tupleLen int
		for tuplePos < dataPos+dataLen {
			tuplePos, tupleLen, list, err = prefix(payload, tuplePos)
			if err != nil {
				return nil, 0, fmt.Errorf("%s: tuple len: %w", ParseTransactionErrorPrefix, err)
			}
			if !list {
				return nil, 0, fmt.Errorf("%s: tuple must be a list, not string", ParseTransactionErrorPrefix)
			}
			if tuplePos+tupleLen > dataPos+dataLen {
				return nil, 0, fmt.Errorf("%s: unexpected end of access list after tuple", ParseTransactionErrorPrefix)
			}
			var addrPos, addrLen int
			addrPos, addrLen, list, err = prefix(payload, tuplePos)
			if err != nil {
				return nil, 0, fmt.Errorf("%s: tuple addr len: %w", ParseTransactionErrorPrefix, err)
			}
			if list {
				return nil, 0, fmt.Errorf("%s: tuple addr must be a string, not list", ParseTransactionErrorPrefix)
			}
			if addrPos+addrLen > tuplePos+tupleLen {
				return nil, 0, fmt.Errorf("%s: unexpected end of tuple after address ", ParseTransactionErrorPrefix)
			}
			if addrLen != 20 {
				return nil, 0, fmt.Errorf("%s: unexpected length of tuple address: %d", ParseTransactionErrorPrefix, addrLen)
			}
			slot.alAddrCount++
			var storagePos, storageLen int
			storagePos, storageLen, list, err = prefix(payload, addrPos+addrLen)
			if err != nil {
				return nil, 0, fmt.Errorf("%s: storage key list len: %w", ParseTransactionErrorPrefix, err)
			}
			if !list {
				return nil, 0, fmt.Errorf("%s: storage key list must be a list, not string", ParseTransactionErrorPrefix)
			}
			if storagePos+storageLen > tuplePos+tupleLen {
				return nil, 0, fmt.Errorf("%s: unexpected end of tuple after storage key list", ParseTransactionErrorPrefix)
			}
			skeyPos := storagePos
			var skeyLen int
			for skeyPos < storagePos+storageLen {
				skeyPos, skeyLen, list, err = prefix(payload, skeyPos)
				if err != nil {
					return nil, 0, fmt.Errorf("%s: tuple storage key len: %w", ParseTransactionErrorPrefix, err)
				}
				if list {
					return nil, 0, fmt.Errorf("%s: tuple storage key must be a string, not list", ParseTransactionErrorPrefix)
				}
				if skeyPos+skeyLen > storagePos+storageLen {
					return nil, 0, fmt.Errorf("%s: unexpected end of tuple after storage key", ParseTransactionErrorPrefix)
				}
				if skeyLen != 32 {
					return nil, 0, fmt.Errorf("%s: unexpected length of tuple storage key: %d", ParseTransactionErrorPrefix, skeyLen)
				}
				slot.alStorCount++
				skeyPos = skeyPos + skeyLen
			}
			if skeyPos != storagePos+storageLen {
				return nil, 0, fmt.Errorf("%s: extraneous space in the tuple after storage key list", ParseTransactionErrorPrefix)
			}
			tuplePos = tuplePos + tupleLen
		}
		if tuplePos != dataPos+dataLen {
			return nil, 0, fmt.Errorf("%s: extraneous space in the access list after all tuples", ParseTransactionErrorPrefix)
		}
		p = dataPos + dataLen
	}
	// This is where the data for sighash ends
	// Next follows V of the signature
	var vByte byte
	sigHashEnd := p
	sigHashLen := uint(sigHashEnd - sigHashPos)
	var chainIdBits, chainIdLen int
	if legacy {
		p, err = parseUint256(payload, p, &ctx.chainId)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: V: %w", ParseTransactionErrorPrefix, err)
		}
		// Compute chainId from V
		if ctx.chainId.Eq(&ctx.n27) || ctx.chainId.Eq(&ctx.n28) {
			// Do not add chain id and two extra zeros
			vByte = byte(ctx.chainId.Uint64() - 27)
		} else {
			ctx.chainId.Sub(&ctx.chainId, &ctx.n35)
			vByte = byte(1 - (ctx.chainId.Uint64() & 1))
			ctx.chainId.Rsh(&ctx.chainId, 1)
			chainIdBits = ctx.chainId.BitLen()
			if chainIdBits <= 7 {
				chainIdLen = 1
			} else {
				chainIdLen = (chainIdBits + 7) / 8 // It is always < 56 bytes
				sigHashLen++                       // For chainId len prefix
			}
			sigHashLen += uint(chainIdLen) // For chainId
			sigHashLen += 2                // For two extra zeros
		}
	} else {
		var v uint64
		p, v, err = parseUint64(payload, p)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: V: %w", ParseTransactionErrorPrefix, err)
		}
		if v > 1 {
			return nil, 0, fmt.Errorf("%s: V is loo large: %d", ParseTransactionErrorPrefix, v)
		}
		vByte = byte(v)
	}
	// Next follows R of the signature
	p, err = parseUint256(payload, p, &ctx.r)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: R: %w", ParseTransactionErrorPrefix, err)
	}
	// New follows S of the signature
	p, err = parseUint256(payload, p, &ctx.s)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: S: %w", ParseTransactionErrorPrefix, err)
	}
	// For legacy transactions, hash the full payload
	if legacy {
		if _, err = ctx.keccak1.Write(payload[pos:p]); err != nil {
			return nil, 0, fmt.Errorf("%s: computing idHash: %w", ParseTransactionErrorPrefix, err)
		}
	}
	ctx.keccak1.Sum(slot.idHash[:0])
	// Computing sigHash (hash used to recover sender from the signature)
	// Write len prefix to the sighash
	if sigHashLen < 56 {
		ctx.buf[0] = byte(sigHashLen) + 192
		if _, err := ctx.keccak2.Write(ctx.buf[:1]); err != nil {
			return nil, 0, fmt.Errorf("%s: computing signHash (hashing len prefix): %w", ParseTransactionErrorPrefix, err)
		}
	} else {
		beLen := (bits.Len(uint(sigHashLen)) + 7) / 8
		binary.BigEndian.PutUint64(ctx.buf[1:], uint64(sigHashLen))
		ctx.buf[8-beLen] = byte(beLen) + 247
		if _, err := ctx.keccak2.Write(ctx.buf[8-beLen : 9]); err != nil {
			return nil, 0, fmt.Errorf("%s: computing signHash (hashing len prefix): %w", ParseTransactionErrorPrefix, err)
		}
	}
	if _, err = ctx.keccak2.Write(payload[sigHashPos:sigHashEnd]); err != nil {
		return nil, 0, fmt.Errorf("%s: computing signHash: %w", ParseTransactionErrorPrefix, err)
	}
	if legacy {
		if chainIdLen > 0 {
			if chainIdBits <= 7 {
				ctx.buf[0] = byte(ctx.chainId.Uint64())
				if _, err := ctx.keccak2.Write(ctx.buf[:1]); err != nil {
					return nil, 0, fmt.Errorf("%s: computing signHash (hashing legacy chainId): %w", ParseTransactionErrorPrefix, err)
				}
			} else {
				binary.BigEndian.PutUint64(ctx.buf[1:9], ctx.chainId[3])
				binary.BigEndian.PutUint64(ctx.buf[9:17], ctx.chainId[2])
				binary.BigEndian.PutUint64(ctx.buf[17:25], ctx.chainId[1])
				binary.BigEndian.PutUint64(ctx.buf[25:33], ctx.chainId[0])
				ctx.buf[32-chainIdLen] = 128 + byte(chainIdLen)
				if _, err = ctx.keccak2.Write(ctx.buf[32-chainIdLen : 33]); err != nil {
					return nil, 0, fmt.Errorf("%s: computing signHash (hashing legacy chainId): %w", ParseTransactionErrorPrefix, err)
				}
			}
			// Encode two zeros
			ctx.buf[0] = 128
			ctx.buf[1] = 128
			if _, err := ctx.keccak2.Write(ctx.buf[:2]); err != nil {
				return nil, 0, fmt.Errorf("%s: computing signHash (hashing zeros after legacy chainId): %w", ParseTransactionErrorPrefix, err)
			}
		}
	}
	// Squeeze sighash
	ctx.keccak2.Sum(ctx.sighash[:0])
	binary.BigEndian.PutUint64(ctx.sig[0:8], ctx.r[3])
	binary.BigEndian.PutUint64(ctx.sig[8:16], ctx.r[2])
	binary.BigEndian.PutUint64(ctx.sig[16:24], ctx.r[1])
	binary.BigEndian.PutUint64(ctx.sig[24:32], ctx.r[0])
	binary.BigEndian.PutUint64(ctx.sig[32:40], ctx.s[3])
	binary.BigEndian.PutUint64(ctx.sig[40:48], ctx.s[2])
	binary.BigEndian.PutUint64(ctx.sig[48:56], ctx.s[1])
	binary.BigEndian.PutUint64(ctx.sig[56:64], ctx.s[0])
	ctx.sig[64] = vByte
	// recover sender
	if _, err = secp256k1.RecoverPubkeyWithContext(ctx.recCtx, ctx.sighash[:], ctx.sig[:], ctx.buf[:0]); err != nil {
		return nil, 0, fmt.Errorf("%s: recovering sender from signature: %w", ParseTransactionErrorPrefix, err)
	}
	// apply keccak to the public key
	ctx.keccak2.Reset()
	if _, err = ctx.keccak2.Write(ctx.buf[1:65]); err != nil {
		return nil, 0, fmt.Errorf("%s: computing sender from public key: %w", ParseTransactionErrorPrefix, err)
	}
	// squeeze the hash of the public key
	ctx.keccak2.Sum(ctx.buf[:0])
	// take last 20 bytes as address
	copy(slot.sender[:], ctx.buf[12:32])
	return &slot, p, nil
}
