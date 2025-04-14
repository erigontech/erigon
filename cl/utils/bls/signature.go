package bls

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync"

	blst "github.com/supranational/blst/bindings/go"
)

// InfiniteSignature represents an infinite signature (G2 Point at Infinity).
var InfiniteSignature = [96]byte{0xC0}

// Length of a BLS signature
const (
	signatureLength = 96
	scalarBytes     = 32
	randBitsEntropy = 64
)

// ETH2 uses BLS12381-G2 Curve
var eth2Curve = []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_")

// Signature wraps CGO object repressenting the signature.
type Signature struct {
	affine *blst.P2Affine
}

// NewSignature creates a new empty signature.
func NewSignature() *Signature {
	return &Signature{affine: new(blst.P2Affine)}
}

// NewSignatureFromBytes creates a new signature from a 96 bytes long slice.
func NewSignatureFromBytes(b []byte) (*Signature, error) {
	if len(b) != signatureLength {
		return nil, fmt.Errorf("bls(signature): invalid signature length. should be %d", signatureLength)
	}
	signature := new(blst.P2Affine).Uncompress(b)
	if signature == nil {
		return nil, ErrDeserializeSignature
	}
	// Group check signature. Do not check for infinity since an aggregated signature
	// could be infinite.
	if !signature.SigValidate(false) {
		return nil, ErrNotGroupSignature
	}
	return &Signature{affine: signature}, nil
}

// VerifyAggregate verify signature against many public keys.
func (s Signature) VerifyAggregate(msg []byte, publicKeys []PublicKey) bool {
	affines := []*blst.P1Affine{}
	for _, publicKey := range publicKeys {
		affines = append(affines, publicKey)
	}
	return s.affine.FastAggregateVerify(true, affines, msg, eth2Curve)
}

// Verify verify signature against one public key.
func (s Signature) Bytes() []byte {
	return s.affine.Compress()
}

// Verify verify signature against one public key.
func (s Signature) Verify(msg []byte, pk PublicKey) bool {
	return s.affine.Verify(false, pk, false, msg, eth2Curve)
}

// VerifyAggregate verify signature against many public keys.
func VerifyAggregate(signature []byte, msg []byte, publicKeysBytes [][]byte) (bool, error) {
	if len(publicKeysBytes) == 0 && bytes.Equal(InfiniteSignature[:], signature) {
		return true, nil
	}
	sig, err := NewSignatureFromBytes(signature)
	if err != nil {
		return false, err
	}

	publicKeys := []PublicKey{}
	for _, publicKey := range publicKeysBytes {
		key, err := NewPublicKeyFromBytes(publicKey)
		if err != nil {
			return false, err
		}
		publicKeys = append(publicKeys, key)
	}

	return sig.VerifyAggregate(msg, publicKeys), nil
}

// Verify verify signature against one public key.
func Verify(signature []byte, msg []byte, publicKeyBytes []byte) (bool, error) {
	sig, err := NewSignatureFromBytes(signature)
	if err != nil {
		return false, err
	}

	publicKey, err := NewPublicKeyFromBytes(publicKeyBytes)
	if err != nil {
		return false, err
	}

	return sig.Verify(msg, publicKey), nil
}

// VerifyMultipleSignatures verifies a non-singular set of signatures and its respective pubkeys and messages.
// This method provides a safe way to verify multiple signatures at once. We pick a number randomly from 1 to max
// uint64 and then multiply the signature by it. We continue doing this for all signatures and its respective pubkeys.
// S* = S_1 * r_1 + S_2 * r_2 + ... + S_n * r_n
// P'_{i,j} = P_{i,j} * r_i
// e(S*, G) = \prod_{i=1}^n \prod_{j=1}^{m_i} e(P'_{i,j}, M_{i,j})
// Using this we can verify multiple signatures safely.
func VerifyMultipleSignatures(sigs [][]byte, msgs [][]byte, pubKeys [][]byte) (bool, error) {
	if len(sigs) == 0 || len(pubKeys) == 0 {
		return false, nil
	}
	rawSigs := new(blst.P2Affine).BatchUncompress(sigs)

	length := len(sigs)
	if length != len(pubKeys) || length != len(msgs) {
		return false, fmt.Errorf("provided signatures, pubkeys and messages have differing lengths. S: %d, P: %d,M %d",
			length, len(pubKeys), len(msgs))
	}
	mulP1Aff := make([]*blst.P1Affine, length)
	rawMsgs := make([]blst.Message, length)

	for i := 0; i < length; i++ {
		pk, err := newPublicKeyFromBytes(pubKeys[i], false)
		if err != nil {
			return false, err
		}
		mulP1Aff[i] = pk
		rawMsgs[i] = msgs[i]
	}
	// Secure source of RNG
	randLock := new(sync.Mutex)

	randFunc := func(scalar *blst.Scalar) {
		var rbytes [scalarBytes]byte
		randLock.Lock()
		rand.Read(rbytes[:])
		randLock.Unlock()
		// Protect against the generator returning 0. Since the scalar value is
		// derived from a big endian byte slice, we take the last byte.
		rbytes[len(rbytes)-1] |= 0x01
		scalar.FromBEndian(rbytes[:])
	}
	dummySig := new(blst.P2Affine)

	// Validate signatures since we uncompress them here. Public keys should already be validated.
	return dummySig.MultipleAggregateVerify(rawSigs, true, mulP1Aff, false, rawMsgs, eth2Curve, randFunc, randBitsEntropy), nil
}

func AggregateSignatures(sigs [][]byte) ([]byte, error) {
	if len(sigs) == 0 {
		return nil, ErrNoSignaturesToAggregate
	}

	agg := new(blst.P2Aggregate)
	mulP2 := make([]*blst.P2Affine, 0, len(sigs))
	for _, pubkey := range sigs {
		sigObj, err := NewSignatureFromBytes(pubkey)
		if err != nil {
			return nil, err
		}
		mulP2 = append(mulP2, sigObj.affine)
	}
	// No group check needed here since it is done in PublicKeyFromBytes
	// Note the checks could be moved from PublicKeyFromBytes into Aggregate
	// and take advantage of multi-threading.
	agg.Aggregate(mulP2, false)
	return agg.ToAffine().Compress(), nil
}
