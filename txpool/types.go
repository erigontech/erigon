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
	"errors"
	"fmt"
	"hash"
	"io"
	"math/bits"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/secp256k1"
	"golang.org/x/crypto/sha3"
)

type TxParsseConfig struct {
	chainID uint256.Int
}

// TxParseContext is object that is required to parse transactions and turn transaction payload into TxSlot objects
// usage of TxContext helps avoid extra memory allocations
type TxParseContext struct {
	keccak1          hash.Hash
	keccak2          hash.Hash
	chainId, r, s, v uint256.Int // Signature values
	chainIDMul       uint256.Int
	deriveChainID    uint256.Int // pre-allocated variable to calculate Sub(&ctx.v, &ctx.chainIDMul)
	buf              [65]byte    // buffer needs to be enough for hashes (32 bytes) and for public key (65 bytes)
	sighash          [32]byte
	sig              [65]byte
	withSender       bool
	isProtected      bool
	checkHash        func([]byte) error

	cfg TxParsseConfig
}

func NewTxParseContext(chainID uint256.Int) *TxParseContext {
	if chainID.IsZero() {
		panic("wrong chainID")
	}
	ctx := &TxParseContext{
		withSender: true,
		keccak1:    sha3.NewLegacyKeccak256(),
		keccak2:    sha3.NewLegacyKeccak256(),
	}

	// behave as of London enabled
	ctx.cfg.chainID.Set(&chainID)
	ctx.chainIDMul.Mul(&chainID, u256.N2)
	return ctx
}

// TxSlot contains information extracted from an Ethereum transaction, which is enough to manage it inside the transaction.
// Also, it contains some auxillary information, like ephemeral fields, and indices within priority queues
type TxSlot struct {
	//txId        uint64      // Transaction id (distinct from transaction hash), used as a compact reference to a transaction accross data structures
	//senderId    uint64      // Sender id (distinct from sender address), used as a compact referecne to to a sender accross data structures
	nonce          uint64      // Nonce of the transaction
	tip            uint64      // Maximum tip that transaction is giving to miner/block proposer
	feeCap         uint64      // Maximum fee that transaction burns and gives to the miner/block proposer
	gas            uint64      // Gas limit of the transaction
	value          uint256.Int // Value transferred by the transaction
	IdHash         [32]byte    // Transaction hash for the purposes of using it as a transaction Id
	senderID       uint64      // SenderID - require external mapping to it's address
	creation       bool        // Set to true if "To" field of the transation is not set
	dataLen        int         // Length of transaction's data (for calculation of intrinsic gas)
	dataNonZeroLen int
	alAddrCount    int // Number of addresses in the access list
	alStorCount    int // Number of storage keys in the access list
	//bestIdx     int         // Index of the transaction in the best priority queue (of whatever pool it currently belongs to)
	//worstIdx    int         // Index of the transaction in the worst priority queue (of whatever pook it currently belongs to)
	//local       bool        // Whether transaction has been injected locally (and hence needs priority when mining or proposing a block)

	rlp []byte
}

const (
	LegacyTxType     int = 0
	AccessListTxType int = 1
	DynamicFeeTxType int = 2
)

const ParseTransactionErrorPrefix = "parse transaction payload"

var ErrRejected = errors.New("rejected")
var ErrAlreadyKnown = errors.New("already known")

func (ctx *TxParseContext) Reject(f func(hash []byte) error) { ctx.checkHash = f }
func (ctx *TxParseContext) WithSender(v bool)                { ctx.withSender = v }

// ParseTransaction extracts all the information from the transactions's payload (RLP) necessary to build TxSlot
// it also performs syntactic validation of the transactions
func (ctx *TxParseContext) ParseTransaction(payload []byte, pos int, slot *TxSlot, sender []byte) (p int, err error) {
	const (
		// txSlotSize is used to calculate how many data slots a single transaction
		// takes up based on its size. The slots are used as DoS protection, ensuring
		// that validating a new transaction remains a constant operation (in reality
		// O(maxslots), where max slots are 4 currently).
		txSlotSize = 32 * 1024

		// txMaxSize is the maximum size a single transaction can have. This field has
		// non-trivial consequences: larger transactions are significantly harder and
		// more expensive to propagate; larger transactions also take more resources
		// to validate whether they fit into the pool or not.
		txMaxSize = 4 * txSlotSize // 128KB
	)
	if len(payload) == 0 {
		return 0, fmt.Errorf("%s: empty rlp", ParseTransactionErrorPrefix)
	}
	if ctx.withSender && len(sender) != 20 {
		return 0, fmt.Errorf("%s: expect sender buffer of len 20", ParseTransactionErrorPrefix)
	}
	// Compute transaction hash
	ctx.keccak1.Reset()
	ctx.keccak2.Reset()
	// Legacy transations have list Prefix, whereas EIP-2718 transactions have string Prefix
	// therefore we assign the first returned value of Prefix function (list) to legacy variable
	dataPos, dataLen, legacy, err := rlp.Prefix(payload, pos)
	if err != nil {
		return 0, fmt.Errorf("%s: size Prefix: %w", ParseTransactionErrorPrefix, err)
	}
	if dataLen > txMaxSize {
		return 0, fmt.Errorf("%s: too large tx.size=%dKb", ParseTransactionErrorPrefix, len(payload)/1024)
	}

	if dataLen == 0 {
		return 0, fmt.Errorf("%s: transaction must be either 1 list or 1 string", ParseTransactionErrorPrefix)
	}
	p = dataPos

	var txType int
	// If it is non-legacy transaction, the transaction type follows, and then the the list
	if !legacy {
		txType = int(payload[p])
		if _, err = ctx.keccak1.Write(payload[p : p+1]); err != nil {
			return 0, fmt.Errorf("%s: computing IdHash (hashing type Prefix): %w", ParseTransactionErrorPrefix, err)
		}
		if _, err = ctx.keccak2.Write(payload[p : p+1]); err != nil {
			return 0, fmt.Errorf("%s: computing signHash (hashing type Prefix): %w", ParseTransactionErrorPrefix, err)
		}
		p++
		if p >= len(payload) {
			return 0, fmt.Errorf("%s: unexpected end of payload after txType", ParseTransactionErrorPrefix)
		}
		dataPos, dataLen, err = rlp.List(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%s: envelope Prefix: %w", ParseTransactionErrorPrefix, err)
		}
		// Hash the envelope, not the full payload
		if _, err = ctx.keccak1.Write(payload[p : dataPos+dataLen]); err != nil {
			return 0, fmt.Errorf("%s: computing IdHash (hashing the envelope): %w", ParseTransactionErrorPrefix, err)
		}
		p = dataPos
	}
	slot.rlp = payload[pos : dataPos+dataLen]

	// Remember where signing hash data begins (it will need to be wrapped in an RLP list)
	sigHashPos := p
	// If it is non-legacy tx, chainId follows, but we skip it
	if !legacy {
		dataPos, dataLen, err = rlp.String(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%s: chainId len: %w", ParseTransactionErrorPrefix, err)
		}
		p = dataPos + dataLen
	}
	// Next follows the nonce, which we need to parse
	p, slot.nonce, err = rlp.U64(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%s: nonce: %w", ParseTransactionErrorPrefix, err)
	}
	// Next follows gas price or tip
	// Although consensus rules specify that tip can be up to 256 bit long, we narrow it to 64 bit
	p, slot.tip, err = rlp.U64(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%s: tip: %w", ParseTransactionErrorPrefix, err)
	}
	// Next follows feeCap, but only for dynamic fee transactions, for legacy transaction, it is
	// equal to tip
	if txType < DynamicFeeTxType {
		slot.feeCap = slot.tip
	} else {
		// Although consensus rules specify that feeCap can be up to 256 bit long, we narrow it to 64 bit
		p, slot.feeCap, err = rlp.U64(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%s: feeCap: %w", ParseTransactionErrorPrefix, err)
		}
	}
	// Next follows gas
	p, slot.gas, err = rlp.U64(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%s: gas: %w", ParseTransactionErrorPrefix, err)
	}
	// Next follows the destrination address (if present)
	dataPos, dataLen, err = rlp.String(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%s: to len: %w", ParseTransactionErrorPrefix, err)
	}
	if dataLen != 0 && dataLen != 20 {
		return 0, fmt.Errorf("%s: unexpected length of to field: %d", ParseTransactionErrorPrefix, dataLen)
	}
	// Only note if To field is empty or not
	slot.creation = dataLen == 0
	p = dataPos + dataLen
	// Next follows value
	p, err = rlp.U256(payload, p, &slot.value)
	if err != nil {
		return 0, fmt.Errorf("%s: value: %w", ParseTransactionErrorPrefix, err)
	}
	// Next goes data, but we are only interesting in its length
	dataPos, dataLen, err = rlp.String(payload, p)
	if err != nil {
		return 0, fmt.Errorf("%s: data len: %w", ParseTransactionErrorPrefix, err)
	}
	slot.dataLen = dataLen

	// Zero and non-zero bytes are priced differently
	slot.dataNonZeroLen = 0
	for _, byt := range payload[dataPos : dataPos+dataLen] {
		if byt != 0 {
			slot.dataNonZeroLen++
		}
	}

	p = dataPos + dataLen

	// Next follows access list for non-legacy transactions, we are only interesting in number of addresses and storage keys
	if !legacy {
		dataPos, dataLen, err = rlp.List(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%s: access list len: %w", ParseTransactionErrorPrefix, err)
		}
		tuplePos := dataPos
		var tupleLen int
		for tuplePos < dataPos+dataLen {
			tuplePos, tupleLen, err = rlp.List(payload, tuplePos)
			if err != nil {
				return 0, fmt.Errorf("%s: tuple len: %w", ParseTransactionErrorPrefix, err)
			}
			var addrPos int
			addrPos, err = rlp.StringOfLen(payload, tuplePos, 20)
			if err != nil {
				return 0, fmt.Errorf("%s: tuple addr len: %w", ParseTransactionErrorPrefix, err)
			}
			slot.alAddrCount++
			var storagePos, storageLen int
			storagePos, storageLen, err = rlp.List(payload, addrPos+20)
			if err != nil {
				return 0, fmt.Errorf("%s: storage key list len: %w", ParseTransactionErrorPrefix, err)
			}
			skeyPos := storagePos
			for skeyPos < storagePos+storageLen {
				skeyPos, err = rlp.StringOfLen(payload, skeyPos, 32)
				if err != nil {
					return 0, fmt.Errorf("%s: tuple storage key len: %w", ParseTransactionErrorPrefix, err)
				}
				slot.alStorCount++
				skeyPos += 32
			}
			if skeyPos != storagePos+storageLen {
				return 0, fmt.Errorf("%s: extraneous space in the tuple after storage key list", ParseTransactionErrorPrefix)
			}
			tuplePos += tupleLen
		}
		if tuplePos != dataPos+dataLen {
			return 0, fmt.Errorf("%s: extraneous space in the access list after all tuples", ParseTransactionErrorPrefix)
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
		p, err = rlp.U256(payload, p, &ctx.v)
		if err != nil {
			return 0, fmt.Errorf("%s: V: %w", ParseTransactionErrorPrefix, err)
		}
		ctx.isProtected = ctx.v.Eq(u256.N27) || ctx.v.Eq(u256.N28)
		// Compute chainId from V
		if ctx.isProtected {
			// Do not add chain id and two extra zeros
			vByte = byte(ctx.v.Uint64() - 27)
			ctx.chainId.Set(&ctx.cfg.chainID)
		} else {
			ctx.chainId.Sub(&ctx.v, u256.N35)
			ctx.chainId.Rsh(&ctx.chainId, 1)
			if ctx.chainId.Cmp(&ctx.cfg.chainID) != 0 {
				return 0, fmt.Errorf("%s: %s, %d (expected %d)", ParseTransactionErrorPrefix, "invalid chainID", ctx.chainId.Uint64(), ctx.cfg.chainID.Uint64())
			}

			chainIdBits = ctx.chainId.BitLen()
			if chainIdBits <= 7 {
				chainIdLen = 1
			} else {
				chainIdLen = (chainIdBits + 7) / 8 // It is always < 56 bytes
				sigHashLen++                       // For chainId len Prefix
			}
			sigHashLen += uint(chainIdLen) // For chainId
			sigHashLen += 2                // For two extra zeros

			//vByte = byte(1 - (ctx.v.Uint64() & 1))
			ctx.deriveChainID.Sub(&ctx.v, &ctx.chainIDMul)
			vByte = ctx.deriveChainID.Sub(&ctx.deriveChainID, u256.N8).Bytes()[0] - 27
		}
	} else {
		var v uint64
		p, v, err = rlp.U64(payload, p)
		if err != nil {
			return 0, fmt.Errorf("%s: V: %w", ParseTransactionErrorPrefix, err)
		}
		if v > 1 {
			return 0, fmt.Errorf("%s: V is loo large: %d", ParseTransactionErrorPrefix, v)
		}
		vByte = byte(v)
		ctx.isProtected = true
		ctx.chainId.Set(&ctx.cfg.chainID)
	}
	if ctx.chainId.Cmp(&ctx.cfg.chainID) != 0 {
		return 0, fmt.Errorf("%s: %s, %d (expected %d)", ParseTransactionErrorPrefix, "invalid chainID", ctx.chainId.Uint64(), ctx.cfg.chainID.Uint64())
	}

	// Next follows R of the signature
	p, err = rlp.U256(payload, p, &ctx.r)
	if err != nil {
		return 0, fmt.Errorf("%s: R: %w", ParseTransactionErrorPrefix, err)
	}
	// New follows S of the signature
	p, err = rlp.U256(payload, p, &ctx.s)
	if err != nil {
		return 0, fmt.Errorf("%s: S: %w", ParseTransactionErrorPrefix, err)
	}

	// For legacy transactions, hash the full payload
	if legacy {
		if _, err = ctx.keccak1.Write(payload[pos:p]); err != nil {
			return 0, fmt.Errorf("%s: computing IdHash: %w", ParseTransactionErrorPrefix, err)
		}
	}
	//ctx.keccak1.Sum(slot.IdHash[:0])
	_, _ = ctx.keccak1.(io.Reader).Read(slot.IdHash[:32])
	if !ctx.withSender {
		return p, nil
	}
	if ctx.checkHash != nil {
		if err := ctx.checkHash(slot.IdHash[:32]); err != nil {
			return p, err
		}
	}

	// Computing sigHash (hash used to recover sender from the signature)
	// Write len Prefix to the sighash
	if sigHashLen < 56 {
		ctx.buf[0] = byte(sigHashLen) + 192
		if _, err := ctx.keccak2.Write(ctx.buf[:1]); err != nil {
			return 0, fmt.Errorf("%s: computing signHash (hashing len Prefix): %w", ParseTransactionErrorPrefix, err)
		}
	} else {
		beLen := (bits.Len(sigHashLen) + 7) / 8
		binary.BigEndian.PutUint64(ctx.buf[1:], uint64(sigHashLen))
		ctx.buf[8-beLen] = byte(beLen) + 247
		if _, err := ctx.keccak2.Write(ctx.buf[8-beLen : 9]); err != nil {
			return 0, fmt.Errorf("%s: computing signHash (hashing len Prefix): %w", ParseTransactionErrorPrefix, err)
		}
	}
	if _, err = ctx.keccak2.Write(payload[sigHashPos:sigHashEnd]); err != nil {
		return 0, fmt.Errorf("%s: computing signHash: %w", ParseTransactionErrorPrefix, err)
	}
	if legacy {
		if chainIdLen > 0 {
			if chainIdBits <= 7 {
				ctx.buf[0] = byte(ctx.chainId.Uint64())
				if _, err := ctx.keccak2.Write(ctx.buf[:1]); err != nil {
					return 0, fmt.Errorf("%s: computing signHash (hashing legacy chainId): %w", ParseTransactionErrorPrefix, err)
				}
			} else {
				binary.BigEndian.PutUint64(ctx.buf[1:9], ctx.chainId[3])
				binary.BigEndian.PutUint64(ctx.buf[9:17], ctx.chainId[2])
				binary.BigEndian.PutUint64(ctx.buf[17:25], ctx.chainId[1])
				binary.BigEndian.PutUint64(ctx.buf[25:33], ctx.chainId[0])
				ctx.buf[32-chainIdLen] = 128 + byte(chainIdLen)
				if _, err = ctx.keccak2.Write(ctx.buf[32-chainIdLen : 33]); err != nil {
					return 0, fmt.Errorf("%s: computing signHash (hashing legacy chainId): %w", ParseTransactionErrorPrefix, err)
				}
			}
			// Encode two zeros
			ctx.buf[0] = 128
			ctx.buf[1] = 128
			if _, err := ctx.keccak2.Write(ctx.buf[:2]); err != nil {
				return 0, fmt.Errorf("%s: computing signHash (hashing zeros after legacy chainId): %w", ParseTransactionErrorPrefix, err)
			}
		}
	}
	// Squeeze sighash
	_, _ = ctx.keccak2.(io.Reader).Read(ctx.sighash[:32])
	//ctx.keccak2.Sum(ctx.sighash[:0])
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
	if _, err = secp256k1.RecoverPubkeyWithContext(secp256k1.DefaultContext, ctx.sighash[:], ctx.sig[:], ctx.buf[:0]); err != nil {
		return 0, fmt.Errorf("%s: recovering sender from signature: %w", ParseTransactionErrorPrefix, err)
	}
	//apply keccak to the public key
	ctx.keccak2.Reset()
	if _, err = ctx.keccak2.Write(ctx.buf[1:65]); err != nil {
		return 0, fmt.Errorf("%s: computing sender from public key: %w", ParseTransactionErrorPrefix, err)
	}
	// squeeze the hash of the public key
	//ctx.keccak2.Sum(ctx.buf[:0])
	_, _ = ctx.keccak2.(io.Reader).Read(ctx.buf[:32])
	//take last 20 bytes as address
	copy(sender, ctx.buf[12:32])
	return p, nil
}

type PeerID *types.H512

type Hashes []byte // flatten list of 32-byte hashes

func (h Hashes) At(i int) []byte { return h[i*length.Hash : (i+1)*length.Hash] }
func (h Hashes) Len() int        { return len(h) / length.Hash }

type Addresses []byte // flatten list of 20-byte addresses

func (h Addresses) At(i int) []byte { return h[i*length.Addr : (i+1)*length.Addr] }
func (h Addresses) Len() int        { return len(h) / length.Addr }

type TxSlots struct {
	txs     []*TxSlot
	senders Addresses
	isLocal []bool
}

func (s TxSlots) Valid() error {
	if len(s.txs) != len(s.isLocal) {
		return fmt.Errorf("TxSlots: expect equal len of isLocal=%d and txs=%d", len(s.isLocal), len(s.txs))
	}
	if len(s.txs) != s.senders.Len() {
		return fmt.Errorf("TxSlots: expect equal len of senders=%d and txs=%d", s.senders.Len(), len(s.txs))
	}
	return nil
}

var zeroAddr = make([]byte, 20)

// Resize internal arrays to len=targetSize, shrinks if need. It rely on `append` algorithm to realloc
func (s *TxSlots) Resize(targetSize uint) {
	for uint(len(s.txs)) < targetSize {
		s.txs = append(s.txs, nil)
	}
	for uint(s.senders.Len()) < targetSize {
		s.senders = append(s.senders, addressesGrowth...)
	}
	for uint(len(s.isLocal)) < targetSize {
		s.isLocal = append(s.isLocal, false)
	}
	//todo: set nil to overflow txs
	oldLen := uint(len(s.txs))
	s.txs = s.txs[:targetSize]
	for i := oldLen; i < targetSize; i++ {
		s.txs[i] = nil
	}
	s.senders = s.senders[:length.Addr*targetSize]
	for i := oldLen; i < targetSize; i++ {
		copy(s.senders.At(int(i)), zeroAddr)
	}
	s.isLocal = s.isLocal[:targetSize]
	for i := oldLen; i < targetSize; i++ {
		s.isLocal[i] = false
	}
}
func (s *TxSlots) Append(slot *TxSlot, sender []byte, isLocal bool) {
	n := len(s.txs)
	s.Resize(uint(len(s.txs) + 1))
	s.txs[n] = slot
	s.isLocal[n] = isLocal
	copy(s.senders.At(n), sender)
}

type TxsRlp struct {
	Txs     [][]byte
	Senders Addresses
	IsLocal []bool
}

// Resize internal arrays to len=targetSize, shrinks if need. It rely on `append` algorithm to realloc
func (s *TxsRlp) Resize(targetSize uint) {
	for uint(len(s.Txs)) < targetSize {
		s.Txs = append(s.Txs, nil)
	}
	for uint(s.Senders.Len()) < targetSize {
		s.Senders = append(s.Senders, addressesGrowth...)
	}
	for uint(len(s.IsLocal)) < targetSize {
		s.IsLocal = append(s.IsLocal, false)
	}
	//todo: set nil to overflow txs
	s.Txs = s.Txs[:targetSize]
	s.Senders = s.Senders[:length.Addr*targetSize]
	s.IsLocal = s.IsLocal[:targetSize]
}

var addressesGrowth = make([]byte, length.Addr)

func EncodeSenderLengthForStorage(nonce uint64, balance uint256.Int) uint {
	var structLength uint = 1 // 1 byte for fieldset
	if !balance.IsZero() {
		structLength += uint(balance.ByteLen()) + 1
	}
	if nonce > 0 {
		structLength += uint((bits.Len64(nonce)+7)/8) + 1
	}
	return structLength
}

func EncodeSender(nonce uint64, balance uint256.Int, buffer []byte) {
	var fieldSet = 0 // start with first bit set to 0
	var pos = 1
	if nonce > 0 {
		fieldSet = 1
		nonceBytes := (bits.Len64(nonce) + 7) / 8
		buffer[pos] = byte(nonceBytes)
		var nonce = nonce
		for i := nonceBytes; i > 0; i-- {
			buffer[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}

	// Encoding balance
	if !balance.IsZero() {
		fieldSet |= 2
		balanceBytes := balance.ByteLen()
		buffer[pos] = byte(balanceBytes)
		pos++
		balance.WriteToSlice(buffer[pos : pos+balanceBytes])
		pos += balanceBytes //nolint
	}

	buffer[0] = byte(fieldSet)
}
func DecodeSender(enc []byte) (nonce uint64, balance uint256.Int, err error) {
	if len(enc) == 0 {
		return
	}

	var fieldSet = enc[0]
	var pos = 1

	if fieldSet&1 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return nonce, balance, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		nonce = bytesToUint64(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&2 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return nonce, balance, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		(&balance).SetBytes(enc[pos+1 : pos+decodeLength+1])
	}
	return
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

//nolint
func (tx *TxSlot) printDebug(prefix string) {
	fmt.Printf("%s: senderID=%d,nonce=%d,tip=%d,v=%d\n", prefix, tx.senderID, tx.nonce, tx.tip, tx.value.Uint64())
	//fmt.Printf("%s: senderID=%d,nonce=%d,tip=%d,hash=%x\n", prefix, tx.senderID, tx.nonce, tx.tip, tx.IdHash)
}

// AccessList is an EIP-2930 access list.
type AccessList []AccessTuple

// AccessTuple is the element type of an access list.
type AccessTuple struct {
	Address     [20]byte   `json:"address"        gencodec:"required"`
	StorageKeys [][32]byte `json:"storageKeys"    gencodec:"required"`
}

// StorageKeys returns the total number of storage keys in the access list.
func (al AccessList) StorageKeys() int {
	sum := 0
	for _, tuple := range al {
		sum += len(tuple.StorageKeys)
	}
	return sum
}
