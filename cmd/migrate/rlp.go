package main

import (
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

func decodeRLP(buf []byte) (accounts.Account, error) {
	acc := accounts.Account{}
	length, structure, pos := decodeLength(buf, 0)
	if pos+length != len(buf) {
		return accounts.Account{}, fmt.Errorf(
			"malformed RLP for Account(%x): prefixLength(%d) + dataLength(%d) != sliceLength(%d)",
			buf, pos, length, len(buf))
	}
	if !structure {
		return accounts.Account{}, fmt.Errorf(
			"encoding of Account should be RLP struct, got byte array: %x",
			buf,
		)
	}

	acc.Initialised = true
	acc.Nonce = 0
	acc.Balance.SetInt64(0)
	acc.Root = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	acc.CodeHash = crypto.Keccak256Hash(nil)
	acc.StorageSize = 0
	acc.HasStorageSize = false
	if length == 0 && structure {
		return accounts.Account{}, nil
	}

	if pos < len(buf) {
		nonceBytes, s, newPos := decodeLength(buf, pos)
		if s {
			return accounts.Account{}, fmt.Errorf(
				"encoding of Account.Nonce should be byte array, got RLP struct: %x",
				buf[pos:newPos+nonceBytes],
			)
		}

		if newPos+nonceBytes > len(buf) {
			return accounts.Account{}, fmt.Errorf(
				"malformed RLP for Account.Nonce(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				buf[pos:newPos+nonceBytes],
				newPos-pos, nonceBytes, len(buf)-pos,
			)
		}

		var nonce uint64
		if nonceBytes == 0 && newPos == pos {
			nonce = uint64(buf[newPos])
			pos = newPos + 1
		} else {
			for _, b := range buf[newPos : newPos+nonceBytes] {
				nonce = (nonce << 8) + uint64(b)
			}
			pos = newPos + nonceBytes
		}

		acc.Nonce = nonce
	}
	if pos < len(buf) {
		balanceBytes, s, newPos := decodeLength(buf, pos)
		if s {
			return accounts.Account{}, fmt.Errorf(
				"encoding of Account.Balance should be byte array, got RLP struct: %x",
				buf[pos:newPos+balanceBytes],
			)
		}

		if newPos+balanceBytes > len(buf) {
			return accounts.Account{}, fmt.Errorf("malformed RLP for Account.Balance(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				buf[pos],
				newPos-pos, balanceBytes, len(buf)-pos,
			)
		}

		var bigB big.Int
		if balanceBytes == 0 && newPos == pos {
			acc.Balance.SetInt64(int64(buf[newPos]))
			pos = newPos + 1
		} else {
			for _, b := range buf[newPos : newPos+balanceBytes] {
				acc.Balance.Lsh(&acc.Balance, 8)
				bigB.SetUint64(uint64(b))
				acc.Balance.Add(&acc.Balance, &bigB)
			}
			pos = newPos + balanceBytes
		}
	}

	// Skip incarnation decoding if this is not read from storage
	if pos < len(buf) {
		incarnationBytes, s, newPos := decodeLength(buf, pos)
		if s {
			return accounts.Account{}, fmt.Errorf(
				"encoding of Account.Incarnation should be byte array, got RLP struct: %x",
				buf[pos:newPos+incarnationBytes],
			)
		}

		if newPos+incarnationBytes > len(buf) {
			return accounts.Account{}, fmt.Errorf(
				"malformed RLP for Account.Incarnation(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				buf[pos:newPos+incarnationBytes],
				newPos-pos, incarnationBytes, len(buf)-pos,
			)
		}

		var incarnation uint64
		if incarnationBytes == 0 && newPos == pos {
			incarnation = uint64(buf[newPos])
			pos = newPos + 1
		} else {
			for _, b := range buf[newPos : newPos+incarnationBytes] {
				incarnation = (incarnation << 8) + uint64(b)
			}
			pos = newPos + incarnationBytes
		}

		acc.Incarnation = incarnation
	}

	if pos < len(buf) {
		rootBytes, s, newPos := decodeLength(buf, pos)
		if s {
			return accounts.Account{}, fmt.Errorf(
				"encoding of Account.Root should be byte array, got RLP struct: %x",
				buf[pos:newPos+rootBytes],
			)
		}

		if rootBytes != 32 {
			return accounts.Account{}, fmt.Errorf(
				"encoding of Account.Root should have size 32, got %d",
				rootBytes,
			)
		}

		if newPos+rootBytes > len(buf) {
			return accounts.Account{}, fmt.Errorf("malformed RLP for Account.Root(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				buf[pos],
				newPos-pos, rootBytes, len(buf)-pos,
			)
		}

		copy(acc.Root[:], buf[newPos:newPos+rootBytes])
		pos = newPos + rootBytes
	}

	if pos < len(buf) {
		codeHashBytes, s, newPos := decodeLength(buf, pos)
		if s {
			return accounts.Account{}, fmt.Errorf(
				"encoding of Account.CodeHash should be byte array, got RLP struct: %x",
				buf[pos:newPos+codeHashBytes],
			)
		}

		if codeHashBytes != 32 {
			return accounts.Account{}, fmt.Errorf(
				"encoding of Account.CodeHash should have size 32, got %d",
				codeHashBytes,
			)
		}

		if newPos+codeHashBytes > len(buf) {
			return accounts.Account{}, fmt.Errorf("malformed RLP for Account.CodeHash(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				buf[pos:newPos+codeHashBytes],
				newPos-pos, codeHashBytes, len(buf)-pos,
			)
		}

		copy(acc.CodeHash[:], buf[newPos:newPos+codeHashBytes])
		pos = newPos + codeHashBytes
	}

	if pos < len(buf) {
		storageSizeBytes, s, newPos := decodeLength(buf, pos)
		if s {
			return accounts.Account{}, fmt.Errorf(
				"encoding of Account.StorageSize should be byte array, got RLP struct: %x",
				buf[pos:newPos+storageSizeBytes],
			)
		}

		if newPos+storageSizeBytes > len(buf) {
			return accounts.Account{}, fmt.Errorf(
				"malformed RLP for Account.StorageSize(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				buf[pos:newPos+storageSizeBytes],
				newPos-pos, storageSizeBytes, len(buf)-pos,
			)
		}

		var storageSize uint64
		if storageSizeBytes == 0 && newPos == pos {
			storageSize = uint64(buf[newPos])
			// Commented out because of the ineffectual assignment - uncomment if adding more fields
			//pos = newPos + 1
		} else {
			for _, b := range buf[newPos : newPos+storageSizeBytes] {
				storageSize = (storageSize << 8) + uint64(b)
			}
			// Commented out because of the ineffectual assignment - uncomment if adding more fields
			//pos = newPos + storageSizeBytes
		}

		acc.StorageSize = storageSize
		acc.HasStorageSize = true
	}
	return acc, nil
}

func decodeLength(buffer []byte, pos int) (length int, structure bool, newPos int) {
	switch firstByte := int(buffer[pos]); {
	case firstByte < 128:
		return 0, false, pos
	case firstByte < 184:
		return firstByte - 128, false, pos + 1
	case firstByte < 192:
		// Next byte is the length of the length + 183
		lenEnd := pos + 1 + firstByte - 183
		len := 0
		for i := pos + 1; i < lenEnd; i++ {
			len = (len << 8) + int(buffer[i])
		}
		return len, false, lenEnd
	case firstByte < 248:
		return firstByte - 192, true, pos + 1
	default:
		// Next byte is the length of the length + 247
		lenEnd := pos + 1 + firstByte - 247
		len := 0
		for i := pos + 1; i < lenEnd; i++ {
			len = (len << 8) + int(buffer[i])
		}
		return len, true, lenEnd
	}
}
