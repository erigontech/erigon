package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	rlp2 "github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rlp"
)

type Authorization struct {
	ChainID uint64
	Address libcommon.Address
	Nonce   uint64
	YParity uint8
	R       uint256.Int
	S       uint256.Int
}

func (ath *Authorization) copy() *Authorization {
	return &Authorization{
		ChainID: ath.ChainID,
		Address: ath.Address,
		Nonce:   ath.Nonce,
		YParity: ath.YParity,
		R:       *ath.R.Clone(),
		S:       *ath.S.Clone(),
	}
}

func (ath *Authorization) RecoverSigner(data *bytes.Buffer, b []byte) (*libcommon.Address, error) {
	authLen := rlp2.U64Len(ath.ChainID)
	authLen += (1 + length.Addr)
	authLen += rlp2.U64Len(ath.Nonce)

	if err := EncodeStructSizePrefix(authLen, data, b); err != nil {
		return nil, err
	}

	// chainId, address, nonce
	if err := rlp.EncodeInt(ath.ChainID, data, b); err != nil {
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

	if ath.Nonce == 1<<64-1 {
		return nil, errors.New("failed assertion: auth.nonce < 2**64 - 1")
	}
	if ath.YParity == 0 || ath.YParity == 1 {
		sig[64] = ath.YParity
	} else {
		return nil, fmt.Errorf("invalid y parity value: %d", ath.YParity)
	}

	if !crypto.TransactionSignatureIsValid(sig[64], &ath.R, &ath.S, false /* allowPreEip2s */) {
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

func authorizationSize(auth Authorization) (authLen int) {
	authLen = rlp2.U64Len(auth.ChainID)
	authLen += rlp2.U64Len(auth.Nonce)
	authLen += (1 + length.Addr)

	authLen += rlp2.U64Len(uint64(auth.YParity)) + (1 + rlp.Uint256LenExcludingHead(&auth.R)) + (1 + rlp.Uint256LenExcludingHead(&auth.S))

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
		if auth.ChainID, err = s.Uint(); err != nil {
			return err
		}

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

		// yParity
		var yParity uint64
		if yParity, err = s.Uint(); err != nil {
			return err
		}
		if yParity >= 1<<8 {
			return fmt.Errorf("authorizations: y parity it too big: %d", yParity)
		}
		auth.YParity = uint8(yParity)

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
		if err := rlp.EncodeInt(auth.ChainID, w, b); err != nil {
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
		// 4. encode YParity, R, S
		if err := rlp.EncodeInt(uint64(auth.YParity), w, b); err != nil {
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
