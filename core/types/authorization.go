package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

type Authorization struct {
	ChainID uint64            `json:"chainId"`
	Address libcommon.Address `json:"address"`
	Nonce   *uint256.Int      `json:"nonce,omitempty"`
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
		cpy.Nonce = ath.Nonce.Clone()
	}

	return cpy
}

func (ath *Authorization) RecoverSigner(data *bytes.Buffer, b []byte) (*libcommon.Address, error) {
	authLen := rlp2.U64Len(ath.ChainID)
	authLen += (1 + length.Addr)
	authLen += 1 // nonce
	if ath.Nonce != nil {
		authLen += rlp.Uint256LenExcludingHead(ath.Nonce)
	}

	if err := rlp.EncodeStringSizePrefix(authLen, data, b[:]); err != nil {
		return nil, err
	}

	// chainId, address, nonce
	if err := rlp.EncodeInt(ath.ChainID, data, b[:]); err != nil {
		return nil, err
	}

	if err := rlp.EncodeAddress(&ath.Address, data, b[:]); err != nil {
		return nil, err
	}

	if err := ath.Nonce.EncodeRLP(data); err != nil {
		return nil, err
	}

	hashData := []byte{params.SetCodeMagicPrefix} //data.Bytes()
	hashData = append(hashData, data.Bytes()...)
	hash := crypto.Keccak256Hash(hashData)

	var sig [65]byte
	var arr [32]byte

	ath.R.WriteToArray32(&arr)
	copy(sig[:32], arr[:])

	ath.S.WriteToArray32(&arr)
	copy(sig[32:], arr[:])

	if ath.V.Eq(u256.Num0) || ath.V.Eq(u256.Num1) {
		sig[64] = byte(ath.V.Uint64())
	} else {
		return nil, fmt.Errorf("invalid V value: %d", ath.V.Uint64())
	}

	pubkey, err := crypto.Ecrecover(hash.Bytes(), sig[:])
	if err != nil {
		return nil, err
	}

	var authority libcommon.Address
	copy(authority[:], crypto.Keccak256(pubkey[1:])[12:])
	return &authority, nil
}

func authorizationsSize(authorizations []Authorization) int {
	totalSize := 0
	// ChainID uint64
	// Address common.Address
	// Nonce   *uint256.Int
	// V, R, S uint256.Int // signature values
	for _, auth := range authorizations {
		size := rlp2.U64Len(auth.ChainID) // chainId
		size += 1 + length.Addr           // address

		size++
		if auth.Nonce != nil {
			size += rlp.Uint256LenExcludingHead(auth.Nonce)
		}

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
		if auth.ChainID, err = s.Uint(); err != nil {
			return err
		}

		if b, err = s.Bytes(); err != nil {
			return err
		}

		if len(b) != 20 {
			return fmt.Errorf("wrong size for Address: %d", len(b))
		}
		auth.Address = libcommon.BytesToAddress(b)

		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		if len(b) > 0 {
			auth.Nonce = new(uint256.Int).SetBytes(b)
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
	// Authorization:
	// 	ChainID uint64
	// 	Address common.Address
	// 	Nonce   *uint256.Int
	// 	V, R, S uint256.Int
	for _, auth := range authorizations {
		// 0. encode length of individual Authorization
		authLen := rlp2.U64Len(auth.ChainID)
		authLen += (1 + length.Addr)
		authLen += 1 // nonce prefix
		if auth.Nonce != nil {
			authLen += rlp.Uint256LenExcludingHead(auth.Nonce)
		}

		authLen += (1 + rlp.Uint256LenExcludingHead(&auth.V)) + (1 + rlp.Uint256LenExcludingHead(&auth.R)) + (1 + rlp.Uint256LenExcludingHead(&auth.S))

		if err := EncodeStructSizePrefix(authLen, w, b); err != nil {
			return err
		}

		// 1. encode ChainId
		if err := rlp.EncodeInt(auth.ChainID, w, b); err != nil {
			return err
		}
		// 2. encode Address
		if err := rlp.EncodeAddress(&auth.Address, w, b); err != nil {
			return err
		}
		// 3. encode Nonce
		if err := auth.Nonce.EncodeRLP(w); err != nil {
			return err
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
