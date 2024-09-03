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
	ChainID *uint256.Int      `json:"chainId"`
	Address libcommon.Address `json:"address"`
	Nonce   uint64            `json:"nonce"`
	V       uint256.Int       `json:"v"`
	R       uint256.Int       `json:"r"`
	S       uint256.Int       `json:"s"`
}

func (ath *Authorization) copy() *Authorization {
	return &Authorization{
		ChainID: ath.ChainID,
		Address: ath.Address,
		Nonce:   ath.Nonce,
		V:       *ath.V.Clone(),
		R:       *ath.R.Clone(),
		S:       *ath.S.Clone(),
	}
}

func (ath *Authorization) RecoverSigner(data *bytes.Buffer, b []byte) (*libcommon.Address, error) {
	authLen := 1 + rlp.Uint256LenExcludingHead(ath.ChainID)
	authLen += (1 + length.Addr)
	authLen += rlp2.U64Len(ath.Nonce)

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

	if err := rlp.EncodeInt(ath.Nonce, data, b); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("invalid v value: %d", ath.V.Uint64())
	}

	if !crypto.ValidateSignatureValues(sig[64], &ath.R, &ath.S, false) {
		return nil, fmt.Errorf("invalid signature")
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

func authorizationSize(auth Authorization) (authLen int) {
	authLen = 1 + rlp.Uint256LenExcludingHead(auth.ChainID)
	authLen += rlp2.U64Len(auth.Nonce)
	authLen += (1 + length.Addr)

	authLen += (1 + rlp.Uint256LenExcludingHead(&auth.V)) + (1 + rlp.Uint256LenExcludingHead(&auth.R)) + (1 + rlp.Uint256LenExcludingHead(&auth.S))

	return
}

func authorizationsSize(authorizations []Authorization) (totalSize int) {
	for _, auth := range authorizations {
		authLen := authorizationSize(auth)
		totalSize += rlp2.ListPrefixLen(authLen) + authLen
	}

	return
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

		// chainId
		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.ChainID = new(uint256.Int).SetBytes(b)

		// address
		if b, err = s.Bytes(); err != nil {
			return err
		}

		if len(b) != 20 {
			return fmt.Errorf("wrong size for Address: %d", len(b))
		}
		auth.Address = libcommon.BytesToAddress(b)

		// nonce
		if auth.Nonce, err = s.Uint(); err != nil {
			return err
		}

		// v
		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.V.SetBytes(b)

		// r
		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.R.SetBytes(b)

		// s
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
		authLen := authorizationSize(auth)
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
		if err := rlp.EncodeInt(auth.Nonce, w, b); err != nil {
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
