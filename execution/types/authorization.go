package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/rlp"
)

type Authorization struct {
	ChainID uint256.Int
	Address common.Address
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

func (ath *Authorization) RecoverSigner(data *bytes.Buffer, buf []byte) (*common.Address, error) {
	if ath.Nonce == math.MaxUint64 {
		return nil, errors.New("failed assertion: auth.nonce < 2**64 - 1")
	}

	authLen := (1 + rlp.Uint256LenExcludingHead(&ath.ChainID))
	authLen += 1 + length.Addr
	authLen += rlp.U64Len(ath.Nonce)

	if err := rlp.EncodeStructSizePrefix(authLen, data, buf); err != nil {
		return nil, err
	}

	// chainId, address, nonce
	if err := rlp.EncodeUint256(&ath.ChainID, data, buf); err != nil {
		return nil, err
	}

	if err := rlp.EncodeOptionalAddress(&ath.Address, data, buf); err != nil {
		return nil, err
	}

	if err := rlp.EncodeInt(ath.Nonce, data, buf); err != nil {
		return nil, err
	}

	return RecoverSignerFromRLP(data.Bytes(), ath.YParity, ath.R, ath.S)
}

func RecoverSignerFromRLP(rlp []byte, yParity uint8, r uint256.Int, s uint256.Int) (*common.Address, error) {
	hashData := []byte{params.SetCodeMagicPrefix}
	hashData = append(hashData, rlp...)
	hash := crypto.Keccak256Hash(hashData)

	var sig [65]byte
	rBytes := r.Bytes()
	sBytes := s.Bytes()
	copy(sig[32-len(rBytes):32], rBytes)
	copy(sig[64-len(sBytes):64], sBytes)

	if yParity > 1 {
		return nil, fmt.Errorf("invalid y parity value: %d", yParity)
	}
	sig[64] = yParity

	if !crypto.TransactionSignatureIsValid(sig[64], &r, &s, false /* allowPreEip2s */) {
		return nil, errors.New("invalid signature")
	}

	pubKey, err := crypto.Ecrecover(hash.Bytes(), sig[:])
	if err != nil {
		return nil, err
	}
	if len(pubKey) == 0 || pubKey[0] != 4 {
		return nil, errors.New("invalid public key")
	}

	var authority common.Address
	copy(authority[:], crypto.Keccak256(pubKey[1:])[12:])
	return &authority, nil
}

func authorizationSize(auth Authorization) (authLen int) {
	authLen = (1 + rlp.Uint256LenExcludingHead(&auth.ChainID))
	authLen += rlp.U64Len(auth.Nonce)
	authLen += 1 + length.Addr

	authLen += rlp.U64Len(uint64(auth.YParity)) + (1 + rlp.Uint256LenExcludingHead(&auth.R)) + (1 + rlp.Uint256LenExcludingHead(&auth.S))

	return
}

func authorizationsSize(authorizations []Authorization) (totalSize int) {
	for _, auth := range authorizations {
		authLen := authorizationSize(auth)
		totalSize += rlp.ListPrefixLen(authLen) + authLen
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

		var chainId []byte
		if chainId, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.ChainID.SetBytes(chainId)

		// address
		if b, err = s.Bytes(); err != nil {
			return err
		}

		if len(b) != 20 {
			return fmt.Errorf("wrong size for Address: %d", len(b))
		}
		auth.Address = common.BytesToAddress(b)

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
	for i := 0; i < len(authorizations); i++ {
		authLen := authorizationSize(authorizations[i])
		if err := rlp.EncodeStructSizePrefix(authLen, w, b); err != nil {
			return err
		}

		// 1. encode ChainId
		if err := rlp.EncodeUint256(&authorizations[i].ChainID, w, b); err != nil {
			return err
		}
		// 2. encode Address
		if err := rlp.EncodeOptionalAddress(&authorizations[i].Address, w, b); err != nil {
			return err
		}
		// 3. encode Nonce
		if err := rlp.EncodeInt(authorizations[i].Nonce, w, b); err != nil {
			return err
		}
		// 4. encode YParity, R, S
		if err := rlp.EncodeInt(uint64(authorizations[i].YParity), w, b); err != nil {
			return err
		}
		if err := rlp.EncodeUint256(&authorizations[i].R, w, b); err != nil {
			return err
		}
		if err := rlp.EncodeUint256(&authorizations[i].S, w, b); err != nil {
			return err
		}
	}
	return nil
}
