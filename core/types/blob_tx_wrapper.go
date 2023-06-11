package types

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/holiman/uint256"
	"github.com/protolambda/ztyp/codec"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	libkzg "github.com/ledgerwatch/erigon-lib/crypto/kzg"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

// Compressed BLS12-381 G1 element
type KZGCommitment [48]byte

func (p *KZGCommitment) Deserialize(dr *codec.DecodingReader) error {
	if p == nil {
		return errors.New("nil pubkey")
	}
	_, err := dr.Read(p[:])
	return err
}

func (p *KZGCommitment) Serialize(w *codec.EncodingWriter) error {
	return w.Write(p[:])
}

func (KZGCommitment) ByteLength() uint64 {
	return 48
}

func (KZGCommitment) FixedLength() uint64 {
	return 48
}

func (p KZGCommitment) MarshalText() ([]byte, error) {
	return []byte("0x" + hex.EncodeToString(p[:])), nil
}

func (p KZGCommitment) String() string {
	return "0x" + hex.EncodeToString(p[:])
}

func (p *KZGCommitment) UnmarshalText(text []byte) error {
	return hexutility.UnmarshalFixedText("KZGCommitment", text, p[:])
}

func (c KZGCommitment) ComputeVersionedHash() libcommon.Hash {
	return libcommon.Hash(libkzg.KZGToVersionedHash(gokzg4844.KZGCommitment(c)))
}

// Compressed BLS12-381 G1 element
type KZGProof [48]byte

func (p *KZGProof) Deserialize(dr *codec.DecodingReader) error {
	if p == nil {
		return errors.New("nil pubkey")
	}
	_, err := dr.Read(p[:])
	return err
}

func (p *KZGProof) Serialize(w *codec.EncodingWriter) error {
	return w.Write(p[:])
}

func (KZGProof) ByteLength() uint64 {
	return 48
}

func (KZGProof) FixedLength() uint64 {
	return 48
}

func (p KZGProof) MarshalText() ([]byte, error) {
	return []byte("0x" + hex.EncodeToString(p[:])), nil
}

func (p KZGProof) String() string {
	return "0x" + hex.EncodeToString(p[:])
}

func (p *KZGProof) UnmarshalText(text []byte) error {
	return hexutility.UnmarshalFixedText("KZGProof", text, p[:])
}

// BLSFieldElement is the raw bytes representation of a field element
type BLSFieldElement [32]byte

func (p BLSFieldElement) MarshalText() ([]byte, error) {
	return []byte("0x" + hex.EncodeToString(p[:])), nil
}

func (p BLSFieldElement) String() string {
	return "0x" + hex.EncodeToString(p[:])
}

func (p *BLSFieldElement) UnmarshalText(text []byte) error {
	return hexutility.UnmarshalFixedText("BLSFieldElement", text, p[:])
}

// Blob data
type Blob [params.FieldElementsPerBlob * 32]byte

func (blob *Blob) Deserialize(dr *codec.DecodingReader) error {
	if blob == nil {
		return errors.New("cannot decode ssz into nil Blob")
	}

	// We treat the blob as an opaque sequence of bytes
	// and therefore we do not do any validation related to field
	// elements
	if _, err := dr.Read(blob[:]); err != nil {
		return err
	}

	return nil
}

func (blob *Blob) Serialize(w *codec.EncodingWriter) error {
	return w.Write(blob[:])
}

func (blob *Blob) ByteLength() (out uint64) {
	return params.FieldElementsPerBlob * 32
}

func (blob *Blob) FixedLength() uint64 {
	return params.FieldElementsPerBlob * 32
}

func (blob *Blob) MarshalText() ([]byte, error) {
	out := make([]byte, 2+params.FieldElementsPerBlob*32*2)
	copy(out[:2], "0x")
	hex.Encode(out[2:], blob[:])

	return out, nil
}

func (blob *Blob) String() string {
	v, err := blob.MarshalText()
	if err != nil {
		return "<invalid-blob>"
	}
	return string(v)
}

func (blob *Blob) UnmarshalText(text []byte) error {
	if blob == nil {
		return errors.New("cannot decode text into nil Blob")
	}
	l := 2 + params.FieldElementsPerBlob*32*2
	if len(text) != l {
		return fmt.Errorf("expected %d characters but got %d", l, len(text))
	}
	if !(text[0] == '0' && text[1] == 'x') {
		return fmt.Errorf("expected '0x' prefix in Blob string")
	}
	if _, err := hex.Decode(blob[:], text[2:]); err != nil {
		return fmt.Errorf("blob is not formatted correctly: %v", err)
	}

	return nil
}

type BlobKzgs []KZGCommitment

func (li *BlobKzgs) Deserialize(dr *codec.DecodingReader) error {
	return dr.List(func() codec.Deserializable {
		i := len(*li)
		*li = append(*li, KZGCommitment{})
		return &(*li)[i]
	}, 48, params.MaxBlobsPerBlock)
}

func (li BlobKzgs) Serialize(w *codec.EncodingWriter) error {
	return w.List(func(i uint64) codec.Serializable {
		return &li[i]
	}, 48, uint64(len(li)))
}

func (li BlobKzgs) ByteLength() uint64 {
	return uint64(len(li)) * 48
}

func (li BlobKzgs) FixedLength() uint64 {
	return 0
}

func (li BlobKzgs) copy() BlobKzgs {
	cpy := make(BlobKzgs, len(li))
	copy(cpy, li)
	return cpy
}

type KZGProofs []KZGProof

func (li *KZGProofs) Deserialize(dr *codec.DecodingReader) error {
	return dr.List(func() codec.Deserializable {
		i := len(*li)
		*li = append(*li, KZGProof{})
		return &(*li)[i]
	}, 48, params.MaxBlobsPerBlock)
}

func (li KZGProofs) Serialize(w *codec.EncodingWriter) error {
	return w.List(func(i uint64) codec.Serializable {
		return &li[i]
	}, 48, uint64(len(li)))
}

func (li KZGProofs) ByteLength() uint64 {
	return uint64(len(li)) * 48
}

func (li KZGProofs) FixedLength() uint64 {
	return 0
}

func (li KZGProofs) copy() KZGProofs {
	cpy := make(KZGProofs, len(li))
	copy(cpy, li)
	return cpy
}

type Blobs []Blob

func (a *Blobs) Deserialize(dr *codec.DecodingReader) error {
	return dr.List(func() codec.Deserializable {
		i := len(*a)
		*a = append(*a, Blob{})
		return &(*a)[i]
	}, params.FieldElementsPerBlob*32, params.FieldElementsPerBlob)
}

func (a Blobs) Serialize(w *codec.EncodingWriter) error {
	return w.List(func(i uint64) codec.Serializable {
		return &a[i]
	}, params.FieldElementsPerBlob*32, uint64(len(a)))
}

func (a Blobs) ByteLength() (out uint64) {
	return uint64(len(a)) * params.FieldElementsPerBlob * 32
}

func (a *Blobs) FixedLength() uint64 {
	return 0 // it's a list, no fixed length
}

func (blobs Blobs) copy() Blobs {
	cpy := make(Blobs, len(blobs))
	copy(cpy, blobs) // each blob element is an array and gets deep-copied
	return cpy
}

// Return KZG commitments, versioned hashes and the proofs that correspond to these blobs
func (blobs Blobs) ComputeCommitmentsAndProofs() (commitments []KZGCommitment, versionedHashes []libcommon.Hash, proofs []KZGProof, err error) {
	commitments = make([]KZGCommitment, len(blobs))
	proofs = make([]KZGProof, len(blobs))
	versionedHashes = make([]libcommon.Hash, len(blobs))

	kzgCtx := libkzg.Ctx()
	for i, blob := range blobs {
		commitment, err := kzgCtx.BlobToKZGCommitment(gokzg4844.Blob(blob), 1 /*numGoRoutines*/)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not convert blob to commitment: %v", err)
		}

		proof, err := kzgCtx.ComputeBlobKZGProof(gokzg4844.Blob(blob), commitment, 1 /*numGoRoutnes*/)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not compute proof for blob: %v", err)
		}
		commitments[i] = KZGCommitment(commitment)
		proofs[i] = KZGProof(proof)
		versionedHashes[i] = libcommon.Hash(libkzg.KZGToVersionedHash(commitment))
	}

	return commitments, versionedHashes, proofs, nil
}

func toBlobs(_blobs Blobs) []gokzg4844.Blob {
	blobs := make([]gokzg4844.Blob, len(_blobs))
	for i, _blob := range _blobs {
		blobs[i] = gokzg4844.Blob(_blob)
	}
	return blobs
}
func toComms(_comms BlobKzgs) []gokzg4844.KZGCommitment {
	comms := make([]gokzg4844.KZGCommitment, len(_comms))
	for i, _comm := range _comms {
		comms[i] = gokzg4844.KZGCommitment(_comm)
	}
	return comms
}
func toProofs(_proofs KZGProofs) []gokzg4844.KZGProof {
	proofs := make([]gokzg4844.KZGProof, len(_proofs))
	for i, _proof := range _proofs {
		proofs[i] = gokzg4844.KZGProof(_proof)
	}
	return proofs
}

// BlobTxWrapper is the "network representation" of a Blob transaction, that is it includes not
// only the SignedBlobTx but also all the associated blob data.
type BlobTxWrapper struct {
	Tx          SignedBlobTx
	Commitments BlobKzgs
	Blobs       Blobs
	Proofs      KZGProofs
}

func (txw *BlobTxWrapper) Deserialize(dr *codec.DecodingReader) error {
	return dr.Container(&txw.Tx, &txw.Commitments, &txw.Blobs, &txw.Proofs)
}

func (txw *BlobTxWrapper) Serialize(w *codec.EncodingWriter) error {
	return w.Container(&txw.Tx, &txw.Commitments, &txw.Blobs, &txw.Proofs)
}

func (txw *BlobTxWrapper) ByteLength() uint64 {
	return codec.ContainerLength(&txw.Tx, &txw.Commitments, &txw.Blobs, &txw.Proofs)
}

func (txw *BlobTxWrapper) FixedLength() uint64 {
	return 0
}

// validateBlobTransactionWrapper implements validate_blob_transaction_wrapper from EIP-4844
func (txw *BlobTxWrapper) ValidateBlobTransactionWrapper() error {
	blobTx := txw.Tx.Message
	l1 := len(blobTx.BlobVersionedHashes)
	if l1 == 0 {
		return fmt.Errorf("a blob tx must contain at least one blob")
	}
	l2 := len(txw.Commitments)
	l3 := len(txw.Blobs)
	l4 := len(txw.Proofs)
	if l1 != l2 || l1 != l3 || l1 != l4 {
		return fmt.Errorf("lengths don't match %v %v %v %v", l1, l2, l3, l4)
	}
	// the following check isn't strictly necessary as it would be caught by data gas processing
	// (and hence it is not explicitly in the spec for this function), but it doesn't hurt to fail
	// early in case we are getting spammed with too many blobs or there is a bug somewhere:
	if uint64(l1) > params.MaxBlobsPerBlock {
		return fmt.Errorf("number of blobs exceeds max: %v", l1)
	}
	kzgCtx := libkzg.Ctx()
	err := kzgCtx.VerifyBlobKZGProofBatch(toBlobs(txw.Blobs), toComms(txw.Commitments), toProofs(txw.Proofs))
	if err != nil {
		return fmt.Errorf("error during proof verification: %v", err)
	}
	for i, h := range blobTx.BlobVersionedHashes {
		if computed := txw.Commitments[i].ComputeVersionedHash(); computed != h {
			return fmt.Errorf("versioned hash %d supposedly %s but does not match computed %s", i, h, computed)
		}
	}
	return nil
}

// Implement transaction interface
func (txw *BlobTxWrapper) Type() byte               { return txw.Tx.Type() }
func (txw *BlobTxWrapper) GetChainID() *uint256.Int { return txw.Tx.GetChainID() }
func (txw *BlobTxWrapper) GetNonce() uint64         { return txw.Tx.GetNonce() }
func (txw *BlobTxWrapper) GetPrice() *uint256.Int   { return txw.Tx.GetPrice() }
func (txw *BlobTxWrapper) GetTip() *uint256.Int     { return txw.Tx.GetTip() }
func (txw *BlobTxWrapper) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	return txw.Tx.GetEffectiveGasTip(baseFee)
}
func (txw *BlobTxWrapper) GetFeeCap() *uint256.Int         { return txw.Tx.GetFeeCap() }
func (txw *BlobTxWrapper) Cost() *uint256.Int              { return txw.Tx.GetFeeCap() }
func (txw *BlobTxWrapper) GetDataHashes() []libcommon.Hash { return txw.Tx.GetDataHashes() }
func (txw *BlobTxWrapper) GetGas() uint64                  { return txw.Tx.GetGas() }
func (txw *BlobTxWrapper) GetDataGas() uint64              { return txw.Tx.GetDataGas() }
func (txw *BlobTxWrapper) GetValue() *uint256.Int          { return txw.Tx.GetValue() }
func (txw *BlobTxWrapper) Time() time.Time                 { return txw.Tx.Time() }
func (txw *BlobTxWrapper) GetTo() *libcommon.Address       { return txw.Tx.GetTo() }
func (txw *BlobTxWrapper) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	return txw.Tx.AsMessage(s, baseFee, rules)
}
func (txw *BlobTxWrapper) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	return txw.Tx.WithSignature(signer, sig)
}
func (txw *BlobTxWrapper) FakeSign(address libcommon.Address) (Transaction, error) {
	return txw.Tx.FakeSign(address)
}
func (txw *BlobTxWrapper) Hash() libcommon.Hash { return txw.Tx.Hash() }
func (txw *BlobTxWrapper) SigningHash(chainID *big.Int) libcommon.Hash {
	return txw.Tx.SigningHash(chainID)
}
func (txw *BlobTxWrapper) GetData() []byte                  { return txw.Tx.GetData() }
func (txw *BlobTxWrapper) GetAccessList() types2.AccessList { return txw.Tx.GetAccessList() }
func (txw *BlobTxWrapper) Protected() bool                  { return txw.Tx.Protected() }
func (txw *BlobTxWrapper) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return txw.Tx.RawSignatureValues()
}
func (txw *BlobTxWrapper) Sender(s Signer) (libcommon.Address, error) { return txw.Tx.Sender(s) }
func (txw *BlobTxWrapper) GetSender() (libcommon.Address, bool)       { return txw.Tx.GetSender() }
func (txw *BlobTxWrapper) SetSender(address libcommon.Address)        { txw.Tx.SetSender(address) }
func (txw *BlobTxWrapper) IsContractDeploy() bool                     { return txw.Tx.IsContractDeploy() }
func (txw *BlobTxWrapper) Unwrap() Transaction                        { return &txw.Tx }

func (txw BlobTxWrapper) EncodingSize() int {
	envelopeSize := int(codec.ContainerLength(&txw.Tx, &txw.Commitments, &txw.Blobs, &txw.Proofs))
	// Add type byte
	envelopeSize++
	return envelopeSize
}

func (txw *BlobTxWrapper) MarshalBinary(w io.Writer) error {
	var b [33]byte
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	wcodec := codec.NewEncodingWriter(w)
	return txw.Serialize(wcodec)
}

func (txw BlobTxWrapper) EncodeRLP(w io.Writer) error {
	var buf bytes.Buffer
	if err := txw.MarshalBinary(&buf); err != nil {
		return err
	}
	return rlp.Encode(w, buf.Bytes())
}
