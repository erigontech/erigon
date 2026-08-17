package types

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
)

var (
	errAuthNonceOverflow = errors.New("authorization nonce has max value")
	errAuthNilPrivateKey = errors.New("private key is nil")
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

// encodeSigningPayload writes rlp([chain_id, address, nonce]), the RLP portion
// of an EIP-7702 authorization signing preimage. Hashing adds the magic prefix;
// buf is scratch space for the RLP encoders.
func encodeSigningPayload(chainID uint256.Int, address common.Address, nonce uint64, w io.Writer, buf []byte) error {
	authLen := rlp.Uint256Len(chainID)
	authLen += 1 + length.Addr
	authLen += rlp.U64Len(nonce)

	if err := rlp.EncodeListPrefix(authLen, w, buf); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(chainID, w, buf); err != nil {
		return err
	}
	if err := EncodeOptionalAddress(&address, w, buf); err != nil {
		return err
	}
	return rlp.EncodeU64(nonce, w, buf)
}

func authorizationSigningHash(chainID uint256.Int, address common.Address, nonce uint64) common.Hash {
	return prefixedPayloadHash(params.SetCodeMagicPrefix, func(w io.Writer, buf []byte) error {
		return encodeSigningPayload(chainID, address, nonce, w, buf)
	})
}

func (ath *Authorization) RecoverSigner() (common.Address, error) {
	if ath.Nonce == math.MaxUint64 {
		return common.Address{}, errAuthNonceOverflow
	}

	hash := authorizationSigningHash(ath.ChainID, ath.Address, ath.Nonce)
	return recoverSignerFromHash(hash, ath.YParity, ath.R, ath.S)
}

// SignAuthorization returns an EIP-7702 authorization signed by key. The
// address is the delegation target; the zero address requests that an existing
// delegation be cleared when the authorization is applied. It rejects a nil
// key or the maximum uint64 nonce, which cannot be incremented during
// authorization processing.
func SignAuthorization(key *ecdsa.PrivateKey, chainID uint256.Int, address common.Address, nonce uint64) (Authorization, error) {
	if key == nil {
		return Authorization{}, errAuthNilPrivateKey
	}
	if nonce == math.MaxUint64 {
		return Authorization{}, errAuthNonceOverflow
	}

	hash := authorizationSigningHash(chainID, address, nonce)
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		return Authorization{}, err
	}

	auth := Authorization{
		ChainID: chainID,
		Address: address,
		Nonce:   nonce,
		YParity: sig[64],
	}
	auth.R.SetBytes(sig[:32])
	auth.S.SetBytes(sig[32:64])
	return auth, nil
}

func recoverSignerFromHash(hash common.Hash, yParity uint8, r uint256.Int, s uint256.Int) (common.Address, error) {
	var sig [65]byte
	r.PutUint256(sig[:32])
	s.PutUint256(sig[32:64])

	if yParity > 1 {
		return common.Address{}, fmt.Errorf("invalid y parity value: %d", yParity)
	}
	sig[64] = yParity

	if !crypto.TransactionSignatureIsValid(sig[64], &r, &s, false /* allowPreEip2s */) {
		return common.Address{}, errors.New("invalid signature")
	}

	pubKey, err := crypto.Ecrecover(hash[:], sig[:])
	if err != nil {
		return common.Address{}, err
	}
	if len(pubKey) == 0 || pubKey[0] != 4 {
		return common.Address{}, errors.New("invalid public key")
	}

	var authority common.Address
	copy(authority[:], crypto.Keccak256(pubKey[1:])[12:])
	return authority, nil
}

func authorizationSize(auth Authorization) (authLen int) {
	authLen = rlp.Uint256Len(auth.ChainID)
	authLen += rlp.U64Len(auth.Nonce)
	authLen += 1 + length.Addr
	authLen += rlp.U64Len(uint64(auth.YParity)) + rlp.Uint256Len(auth.R) + rlp.Uint256Len(auth.S)
	return
}

func authorizationsSize(authorizations []Authorization) (totalSize int) {
	for i := range authorizations {
		authLen := authorizationSize(authorizations[i])
		totalSize += rlp.ListPrefixLen(authLen) + authLen
	}

	return
}

func decodeAuthorizations(auths *[]Authorization, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open authorizations: %w", err)
	}
	i := 0
	for _, err = s.List(); err == nil; _, err = s.List() {
		auth := Authorization{}

		if err := s.ReadUint256(&auth.ChainID); err != nil {
			return err
		}

		// address
		if auth.Address, err = s.Addr(); err != nil {
			return fmt.Errorf("read Address: %w", err)
		}

		// nonce
		if auth.Nonce, err = s.Uint64(); err != nil {
			return err
		}

		// yParity
		var yParity uint64
		if yParity, err = s.Uint64(); err != nil {
			return err
		}
		if yParity >= 1<<8 {
			return fmt.Errorf("authorizations: y parity it too big: %d", yParity)
		}
		auth.YParity = uint8(yParity)

		// r
		if err := s.ReadUint256(&auth.R); err != nil {
			return err
		}

		// s
		if err := s.ReadUint256(&auth.S); err != nil {
			return err
		}

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
	for i := range authorizations {
		authLen := authorizationSize(authorizations[i])
		if err := rlp.EncodeListPrefix(authLen, w, b); err != nil {
			return err
		}

		// 1. encode ChainId
		if err := rlp.EncodeUint256(authorizations[i].ChainID, w, b); err != nil {
			return err
		}
		// 2. encode Address
		if err := EncodeOptionalAddress(&authorizations[i].Address, w, b); err != nil {
			return err
		}
		// 3. encode Nonce
		if err := rlp.EncodeU64(authorizations[i].Nonce, w, b); err != nil {
			return err
		}
		// 4. encode YParity, R, S
		if err := rlp.EncodeU64(uint64(authorizations[i].YParity), w, b); err != nil {
			return err
		}
		if err := rlp.EncodeUint256(authorizations[i].R, w, b); err != nil {
			return err
		}
		if err := rlp.EncodeUint256(authorizations[i].S, w, b); err != nil {
			return err
		}
	}
	return nil
}
