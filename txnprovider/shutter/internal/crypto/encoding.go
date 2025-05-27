// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package crypto

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	blst "github.com/supranational/blst/bindings/go"

	"github.com/erigontech/erigon-lib/common/hexutil"
)

var (
	ErrInputTooLong             = errors.New("input too long")
	ErrInvalidEonSecretKeyShare = errors.New("invalid eon secret key share")
	ErrVersionMismatch          = func(version_got byte) error {
		return fmt.Errorf("version mismatch. want %d got %d", VersionIdentifier, version_got)
	}
)

// Marshal serializes the EncryptedMessage object. It panics, if C1 is nil.
func (m *EncryptedMessage) Marshal() []byte {
	if m.C1 == nil {
		panic("not a valid encrypted message. C1==nil")
	}

	buff := bytes.Buffer{}
	buff.WriteByte(VersionIdentifier)
	buff.Write(m.C1.Compress())
	buff.Write(m.C2[:])
	for i := range m.C3 {
		buff.Write(m.C3[i][:])
	}

	return buff.Bytes()
}

// Unmarshal deserializes an EncryptedMessage from the given byte slice.
func (m *EncryptedMessage) Unmarshal(d []byte) error {
	if len(d) == 0 {
		return errors.New("not enough data")
	}
	if d[0] != VersionIdentifier {
		return ErrVersionMismatch(IdentifyVersion(d))
	}

	if len(d) < 1+blst.BLST_P2_COMPRESS_BYTES+BlockSize ||
		(len(d)-1-blst.BLST_P2_COMPRESS_BYTES-BlockSize)%BlockSize != 0 {
		return fmt.Errorf("invalid length %d of encrypted message", len(d))
	}
	if m.C1 == nil {
		m.C1 = new(blst.P2Affine)
	}
	m.C1.Uncompress(d[1 : 1+blst.BLST_P2_COMPRESS_BYTES])
	if m.C1 == nil {
		return errors.New("failed to deserialize C1")
	}
	if !m.C1.InG2() {
		return errors.New("C1 not on curve")
	}

	copy(m.C2[:], d[1+blst.BLST_P2_COMPRESS_BYTES:1+blst.BLST_P2_COMPRESS_BYTES+BlockSize])

	m.C3 = nil
	for i := 1 + blst.BLST_P2_COMPRESS_BYTES + BlockSize; i < len(d); i += BlockSize {
		b := Block{}
		copy(b[:], d[i:i+BlockSize])
		m.C3 = append(m.C3, b)
	}

	return nil
}

// Marshal serializes the eon secret key share.
func (eonSecretKeyShare *EonSecretKeyShare) Marshal() []byte {
	return (*big.Int)(eonSecretKeyShare).Bytes()
}

// Unarshal deserializes an eon secret key share.
func (eonSecretKeyShare *EonSecretKeyShare) Unmarshal(m []byte) error {
	(*big.Int)(eonSecretKeyShare).SetBytes(m)
	if (*big.Int)(eonSecretKeyShare).Cmp(order) >= 0 {
		return ErrInvalidEonSecretKeyShare
	}
	return nil
}

// Marshal serializes the eon public key share.
func (eonPublicKeyShare *EonPublicKeyShare) Marshal() []byte {
	return (*blst.P2Affine)(eonPublicKeyShare).Compress()
}

// Unmarshal deserializes an eon public key share.
func (eonPublicKeyShare *EonPublicKeyShare) Unmarshal(m []byte) error {
	p := (*blst.P2Affine)(eonPublicKeyShare)
	p.Uncompress(m)
	if p == nil {
		return errors.New("failed to deserialize eon public key share")
	}
	if !p.InG2() {
		return errors.New("eon public key share is not on curve")
	}
	return nil
}

// Marshal serializes the eon public key.
func (eonPublicKey *EonPublicKey) Marshal() []byte {
	return (*blst.P2Affine)(eonPublicKey).Compress()
}

// Unmarshal deserializes an eon public key from the given byte slice.
func (eonPublicKey *EonPublicKey) Unmarshal(m []byte) error {
	p := (*blst.P2Affine)(eonPublicKey)
	p.Uncompress(m)
	if p == nil {
		return errors.New("failed to deserialize eon public key")
	}
	if !p.InG2() {
		return errors.New("eon public key is not on curve")
	}
	return nil
}

// MarshalText serializes the eon public key to hex.
func (eonPublicKey EonPublicKey) MarshalText() ([]byte, error) {
	return hexutil.Bytes(eonPublicKey.Marshal()).MarshalText()
}

// UnmarshalText deserializes the eon public key from hex.
func (eonPublicKey *EonPublicKey) UnmarshalText(input []byte) error {
	var b hexutil.Bytes
	if err := b.UnmarshalText(input); err != nil {
		return err
	}
	return eonPublicKey.Unmarshal(b)
}

// Marshal serializes the epoch id.
func (epochID *EpochID) Marshal() []byte {
	return (*blst.P1Affine)(epochID).Compress()
}

// Unmarshal deserializes an epoch id.
func (epochID *EpochID) Unmarshal(m []byte) error {
	p := (*blst.P1Affine)(epochID)
	p.Uncompress(m)
	if p == nil {
		return errors.New("failed to deserialize epoch id")
	}
	if !p.InG1() {
		return errors.New("epoch id is not on curve")
	}
	*epochID = EpochID(*p)
	return nil
}

// Marshal serializes the epoch secret key share.
func (epochSecretKeyShare *EpochSecretKeyShare) Marshal() []byte {
	return (*blst.P1Affine)(epochSecretKeyShare).Compress()
}

// Unmarshal deserializes an epoch secret key share.
func (epochSecretKeyShare *EpochSecretKeyShare) Unmarshal(m []byte) error {
	p := (*blst.P1Affine)(epochSecretKeyShare)
	p.Uncompress(m)
	if p == nil {
		return errors.New("failed to deserialize epoch secret key share")
	}
	if !p.InG1() {
		return errors.New("epoch secret key share is not on curve")
	}
	return nil
}

// Marshal serializes the epoch secret key.
func (epochSecretKey *EpochSecretKey) Marshal() []byte {
	return (*blst.P1Affine)(epochSecretKey).Compress()
}

// Unmarshal deserializes an epoch secret key.
func (epochSecretKey *EpochSecretKey) Unmarshal(m []byte) error {
	p := (*blst.P1Affine)(epochSecretKey)
	p.Uncompress(m)
	if p == nil {
		return errors.New("failed to deserialize epoch secret key")
	}
	if !p.InG1() {
		return errors.New("epoch secret key is not on curve")
	}
	return nil
}

// MarshalText serializes the epoch secret key to hex.
func (epochSecretKey EpochSecretKey) MarshalText() ([]byte, error) { //nolint: unparam
	return []byte(hexutil.Encode(epochSecretKey.Marshal())), nil
}

// UnmarshalText deserializes the epoch secret key from hex.
func (epochSecretKey *EpochSecretKey) UnmarshalText(input []byte) error {
	var b hexutil.Bytes
	if err := b.UnmarshalText(input); err != nil {
		return err
	}
	return epochSecretKey.Unmarshal(b)
}

// MarshalText serializes the block to hex.
func (block Block) MarshalText() ([]byte, error) { //nolint:unparam
	return []byte(hexutil.Encode(block[:])), nil
}

// UnmarshalText deserializes the block from hex.
func (block *Block) UnmarshalText(b []byte) error {
	decoded, err := hexutil.Decode(string(b))
	copy(block[:], decoded)
	return err
}

// MarshalText serializes the encrypted message to hex.
func (m EncryptedMessage) MarshalText() ([]byte, error) { //nolint:unparam
	return []byte(hexutil.Encode(m.Marshal())), nil
}

// UnmarshalText deserializes the encrypted message from hex.
func (m *EncryptedMessage) UnmarshalText(b []byte) error {
	decoded, err := hexutil.Decode(string(b))
	if err != nil {
		return err
	}
	err = m.Unmarshal(decoded)
	return err
}

// Marshal serializes the gammas value.
//
// Serialization format: [n:4][gamma1:96]...[gamman:96]
func (g *Gammas) Marshal() []byte {
	if g == nil {
		return []byte{}
	}
	buff := bytes.Buffer{}
	for _, p := range *g {
		buff.Write(p.Compress())
	}
	return buff.Bytes()
}

// Unmarshal deserializes a gammas value.
func (g *Gammas) Unmarshal(m []byte) error {
	if len(m)%blst.BLST_P2_COMPRESS_BYTES != 0 {
		return errors.New("invalid length of gammas")
	}
	n := len(m) / blst.BLST_P2_COMPRESS_BYTES
	*g = make(Gammas, n)
	for i := 0; i < n; i++ {
		p := new(blst.P2Affine)
		p = p.Uncompress(m[i*blst.BLST_P2_COMPRESS_BYTES : (i+1)*blst.BLST_P2_COMPRESS_BYTES])
		if p == nil {
			return errors.New("failed to deserialize gamma")
		}
		if !p.InG2() {
			return errors.New("gamma is not on curve")
		}
		(*g)[i] = p
	}
	return nil
}

// MarshalText serializes the gammas as hex.
func (g *Gammas) MarshalText() ([]byte, error) {
	return []byte(hexutil.Encode(g.Marshal())), nil
}

// UnmarshalText deserializes the gammas from hex.
func (g *Gammas) UnmarshalText(b []byte) error {
	decoded, err := hexutil.Decode(string(b))
	if err != nil {
		return err
	}
	err = g.Unmarshal(decoded)
	return err
}
