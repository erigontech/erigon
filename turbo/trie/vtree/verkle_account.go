package vtree

import (
	"math/big"
	"math/bits"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
)

var emptyCodeHash = crypto.Keccak256Hash(nil)

type VerkleAccount struct {
	Balance  *big.Int
	Nonce    uint64
	CodeSize uint64
	CodeHash common.Hash
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

func AccountToVerkleAccount(account accounts.Account) VerkleAccount {
	return VerkleAccount{
		Balance:  account.Balance.ToBig(),
		Nonce:    account.Nonce,
		CodeHash: account.CodeHash,
	}
}

func (a VerkleAccount) EncodeVerkleAccountForStorage(buf []byte) {
	fieldset := byte(0x00)
	pos := 1
	if a.Balance.Sign() > 0 {
		fieldset = byte(0x01)
		balanceBytes := a.Balance.Bytes()
		buf[pos] = byte(len(balanceBytes))
		pos++
		copy(buf[pos:], balanceBytes)
		pos += len(balanceBytes)
	}

	if a.Nonce > 0 {
		fieldset |= 0x2
		nonceBytes := (bits.Len64(a.Nonce) + 7) / 8
		buf[pos] = byte(nonceBytes)
		var nonce = a.Nonce
		for i := nonceBytes; i > 0; i-- {
			buf[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}

	if a.CodeSize > 0 {
		fieldset |= 0x4
		codeSizeLen := (bits.Len64(a.CodeSize) + 7) / 8
		buf[pos] = byte(codeSizeLen)
		var codeSize = a.CodeSize
		for i := codeSizeLen; i > 0; i-- {
			buf[pos+i] = byte(codeSize)
			codeSize >>= 8
		}
		pos += codeSizeLen + 1
	}

	if a.CodeHash != emptyCodeHash {
		fieldset |= 0x8
		copy(buf[pos:], a.CodeHash[:])
	}
	buf[0] = fieldset
}

func (a VerkleAccount) GetVerkleAccountSizeForStorage() (length int) {
	length = 1
	if a.Balance.Sign() > 0 {
		length += len(a.Balance.Bytes()) + 1
	}
	if a.Nonce > 0 {
		length += ((bits.Len64(a.Nonce) + 7) / 8) + 1
	}
	if a.CodeSize > 0 {
		length += ((bits.Len64(a.CodeSize) + 7) / 8) + 1
	}
	if a.CodeHash != emptyCodeHash {
		length += 32
	}
	return
}

func DecodeVerkleAccountForStorage(buf []byte) VerkleAccount {
	if len(buf) == 0 {
		return VerkleAccount{
			Balance:  common.Big0,
			CodeHash: emptyCodeHash,
		}
	}
	fieldset := buf[0]
	pos := 1
	balance := big.NewInt(0)
	if fieldset&1 > 0 {
		balance.SetBytes(buf[pos+1 : pos+1+int(buf[pos])])
		pos += int(buf[pos]) + 1
	}

	nonce := uint64(0)
	if fieldset&2 > 0 {
		nonce = bytesToUint64(buf[pos+1 : pos+1+int(buf[pos])])
		pos += int(buf[pos]) + 1
	}

	codeSize := uint64(0)
	if fieldset&4 > 0 {
		codeSize = bytesToUint64(buf[pos+1 : pos+1+int(buf[pos])])
		pos += int(buf[pos]) + 1
	}

	if fieldset&8 > 0 {
		return VerkleAccount{
			Balance:  balance,
			Nonce:    nonce,
			CodeHash: common.BytesToHash(buf[pos:]),
			CodeSize: codeSize,
		}
	}

	return VerkleAccount{
		Balance:  balance,
		Nonce:    nonce,
		CodeHash: emptyCodeHash,
		CodeSize: codeSize,
	}
}

func (a VerkleAccount) IsEmptyCodeHash() bool {
	return a.CodeHash == emptyCodeHash
}
