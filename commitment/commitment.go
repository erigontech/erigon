package commitment

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/common/length"
)

// Trie represents commitment variant.
type Trie interface {
	ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (branchNodeUpdates map[string][]byte, err error)

	// RootHash produces root hash of the trie
	RootHash() (hash []byte, err error)

	// Variant returns commitment trie variant
	Variant() TrieVariant

	// Reset Drops everything from the trie
	Reset()

	ResetFns(
		branchFn func(prefix []byte) ([]byte, error),
		accountFn func(plainKey []byte, cell *Cell) error,
		storageFn func(plainKey []byte, cell *Cell) error,
	)

	// Makes trie more verbose
	SetTrace(bool)
}

type TrieVariant string

const (
	// HexPatriciaHashed used as default commitment approach
	VariantHexPatriciaTrie        TrieVariant = "hex-patricia-hashed"
	VariantReducedHexPatriciaTrie TrieVariant = "modified-hex-patricia-hashed"
	// Experimental mode with binary key representation
	VariantBinPatriciaTrie TrieVariant = "bin-patricia-hashed"
)

func InitializeTrie(tv TrieVariant) Trie {
	switch tv {
	case VariantBinPatriciaTrie:
		return NewBinaryPatriciaTrie()
	case VariantReducedHexPatriciaTrie:
		return NewBinHashed(length.Addr, nil, nil, nil)
	case VariantHexPatriciaTrie:
		fallthrough
	default:
		return NewHexPatriciaHashed(length.Addr, nil, nil, nil)
	}
}

type Account struct {
	CodeHash []byte // hash of the bytecode
	Nonce    uint64
	Balance  uint256.Int
}

func (a *Account) String() string {
	return fmt.Sprintf("Account{Nonce: %d, Balance: %s, CodeHash: %x}", a.Nonce, a.Balance.String(), a.CodeHash)
}

func (a *Account) decode(buffer []byte) *Account {
	var pos int
	if buffer[pos] < 128 {
		a.Nonce = uint64(buffer[pos])
		pos++
	} else {
		var nonce uint64
		sizeBytes := int(buffer[pos] - 128)
		pos++
		nonce, n := binary.Uvarint(buffer[pos : pos+sizeBytes])
		a.Nonce = nonce
		pos += n
	}

	if buffer[pos] < 128 {
		b := uint256.NewInt(uint64(buffer[pos]))
		a.Balance = *b
		pos++
	} else {
		bc := int(buffer[pos] - 128)
		pos++
		a.Balance.SetBytes(buffer[pos : pos+bc])
		pos += bc
	}

	codeSize := int(buffer[pos] - 128)
	if codeSize > 0 {
		pos++
		a.CodeHash = make([]byte, codeSize)
		copy(a.CodeHash, buffer[pos:pos+codeSize])
	}
	return a
}

func (a *Account) encode(bb []byte) []byte {
	buffer := bytes.NewBuffer(bb)

	// Encoding nonce
	if a.Nonce < 128 && a.Nonce != 0 {
		buffer.WriteByte(byte(a.Nonce))
	} else {
		aux := [binary.MaxVarintLen64]byte{}
		n := binary.PutUvarint(aux[:], a.Nonce)
		buffer.WriteByte(byte(128 + n))
		buffer.Write(aux[:n])
	}

	// Encoding balance
	if a.Balance.LtUint64(128) && !a.Balance.IsZero() {
		buffer.WriteByte(byte(a.Balance.Uint64()))
	} else {
		buffer.WriteByte(byte(128 + a.Balance.ByteLen()))
		buffer.Write(a.Balance.Bytes())
	}
	buffer.WriteByte(byte(128 + len(a.CodeHash)))
	buffer.Write(a.CodeHash)
	return buffer.Bytes()
}
