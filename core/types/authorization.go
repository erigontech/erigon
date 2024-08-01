package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	rlp2 "github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rlp"
)

type Authorization struct {
	ChainID *uint256.Int      `json:"chainId"`
	Address libcommon.Address `json:"address"`
	Nonce   []uint64          `json:"nonce"`
	V       uint256.Int       `json:"v"`
	R       uint256.Int       `json:"r"`
	S       uint256.Int       `json:"s"`
}

func (ath *Authorization) copy() *Authorization {
	cpy := &Authorization{
		ChainID: ath.ChainID,
		Address: ath.Address,
		V:       *ath.V.Clone(),
		R:       *ath.R.Clone(),
		S:       *ath.S.Clone(),
	}

	if ath.Nonce != nil {
		cpy.Nonce = ath.Nonce
	}

	return cpy
}

func (ath *Authorization) RecoverSigner(data *bytes.Buffer, b []byte) (*libcommon.Address, error) {
	authLen := 1 + rlp.Uint256LenExcludingHead(ath.ChainID)
	authLen += (1 + length.Addr)
	var nonceSize int
	if len(ath.Nonce) == 0 {
		// if empty array, treat as encoding nil
		authLen += 1
	} else {
		nonceSize = noncesSize(ath.Nonce)
		authLen += rlp2.ListPrefixLen(nonceSize) + nonceSize
	}

	if err := EncodeStructSizePrefix(authLen, data, b); err != nil {
		return nil, err
	}

	// chainId, address, nonce
	if err := ath.ChainID.EncodeRLP(data); err != nil {
		return nil, err
	}

	if err := rlp.EncodeOptionalAddress(&ath.Address, data, b); err != nil {
		return nil, err
	}

	if len(ath.Nonce) == 0 {
		if err := rlp.EncodeString(nil, data, b); err != nil {
			return nil, err
		}
	} else {
		if err := EncodeStructSizePrefix(nonceSize, data, b); err != nil {
			return nil, err
		}

		for _, nonce := range ath.Nonce {
			if err := rlp.EncodeInt(nonce, data, b); err != nil {
				return nil, err
			}
		}
	}

	hashData := []byte{params.SetCodeMagicPrefix}
	hashData = append(hashData, data.Bytes()...)
	hash := crypto.Keccak256Hash(hashData)

	var sig [65]byte
	r := ath.R.Bytes()
	s := ath.S.Bytes()
	copy(sig[32-len(r):32], r)
	copy(sig[64-len(s):64], s)

	if ath.V.Eq(u256.Num0) || ath.V.Eq(u256.Num1) {
		sig[64] = byte(ath.V.Uint64())
	} else {
		return nil, fmt.Errorf("invalid V value: %d", ath.V.Uint64())
	}

	if !crypto.ValidateSignatureValues(sig[64], &ath.R, &ath.S, false) {
		return nil, errors.New("invalid signature")
	}

	pubkey, err := crypto.Ecrecover(hash.Bytes(), sig[:])
	if err != nil {
		return nil, err
	}
	if len(pubkey) == 0 || pubkey[0] != 4 {
		return nil, errors.New("invalid public key")
	}

	var authority libcommon.Address
	copy(authority[:], crypto.Keccak256(pubkey[1:])[12:])
	return &authority, nil
}

func authorizationsSize(authorizations []Authorization) int {
	totalSize := 0
	// ChainID uint64
	// Address common.Address
	// Nonce   []uint256.Int
	// V, R, S uint256.Int // signature values
	for _, auth := range authorizations {
		size := 1 + rlp.Uint256LenExcludingHead(auth.ChainID)
		size += 1 + length.Addr // address

		noncesSize := noncesSize(auth.Nonce)
		size += rlp2.ListPrefixLen(noncesSize) + noncesSize

		size++
		size += rlp.Uint256LenExcludingHead(&auth.V)

		size++
		size += rlp.Uint256LenExcludingHead(&auth.R)

		size++
		size += rlp.Uint256LenExcludingHead(&auth.S)

		totalSize += size + rlp2.ListPrefixLen(size)
	}
	return totalSize
}

func decodeAuthorizations(auths *[]Authorization, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open authorizations: %w", err)
	}
	var b []byte
	i := 0
	for _, err = s.List(); err == nil; _, err = s.List() {
		auth := Authorization{}
		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.ChainID = new(uint256.Int).SetBytes(b)

		if b, err = s.Bytes(); err != nil {
			return err
		}

		if len(b) != 20 {
			return fmt.Errorf("wrong size for Address: %d", len(b))
		}
		auth.Address = libcommon.BytesToAddress(b)
		if auth.Nonce, err = decodeNonces(s); err != nil {
			return err
		}

		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.V.SetBytes(b)

		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.R.SetBytes(b)

		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.S.SetBytes(b)

		*auths = append(*auths, auth)
		// end of authorization
		if err = s.ListEnd(); err != nil {
			return fmt.Errorf("close Authorization: %w", err)
		}
		i++
	}
	if !errors.Is(err, rlp.EOL) {
		return fmt.Errorf("open authorizations: %d %w", i, err)
	}
	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close authorizations: %w", err)
	}
	return nil
}

func encodeAuthorizations(authorizations []Authorization, w io.Writer, b []byte) error {
	for _, auth := range authorizations {
		// 0. encode length of individual Authorization
		authLen := 1 + rlp.Uint256LenExcludingHead(auth.ChainID)
		authLen += (1 + length.Addr)
		noncesSize := noncesSize(auth.Nonce)
		authLen += rlp2.ListPrefixLen(noncesSize) + noncesSize

		authLen += (1 + rlp.Uint256LenExcludingHead(&auth.V)) + (1 + rlp.Uint256LenExcludingHead(&auth.R)) + (1 + rlp.Uint256LenExcludingHead(&auth.S))

		if err := EncodeStructSizePrefix(authLen, w, b); err != nil {
			return err
		}

		// 1. encode ChainId
		if err := auth.ChainID.EncodeRLP(w); err != nil {
			return err
		}
		// 2. encode Address
		if err := rlp.EncodeOptionalAddress(&auth.Address, w, b); err != nil {
			return err
		}
		// 3. encode Nonce
		if err := EncodeStructSizePrefix(noncesSize, w, b); err != nil {
			return err
		}

		for _, nonce := range auth.Nonce {
			if err := rlp.EncodeInt(nonce, w, b); err != nil {
				return err
			}
		}

		// 4. encode V, R, S
		if err := auth.V.EncodeRLP(w); err != nil {
			return err
		}
		if err := auth.R.EncodeRLP(w); err != nil {
			return err
		}
		if err := auth.S.EncodeRLP(w); err != nil {
			return err
		}
	}

	return nil
}

func noncesSize(nonce []uint64) int {
	totalSize := 0
	for _, n := range nonce {
		totalSize += rlp.IntLenExcludingHead(n) + 1
	}
	return totalSize
}

func decodeNonces(s *rlp.Stream) ([]uint64, error) {
	_, err := s.List()
	if err != nil {
		return nil, fmt.Errorf("open nonce: %w", err)
	}
	nonces := make([]uint64, 0)
	var b uint64
	i := 0
	for b, err = s.Uint(); err == nil; b, err = s.Uint() {
		nonces = append(nonces, b)
		i++
	}
	if !errors.Is(err, rlp.EOL) {
		return nil, fmt.Errorf("open nonce: %d %w", i, err)
	}
	if err = s.ListEnd(); err != nil {
		return nil, fmt.Errorf("close nonce: %w", err)
	}
	return nonces, nil
}
