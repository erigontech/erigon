package types

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/ethereum/go-ethereum/crypto/kzg"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/protolambda/go-kzg/bls"
	"github.com/protolambda/ztyp/codec"
	"github.com/protolambda/ztyp/tree"
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

func (p KZGCommitment) HashTreeRoot(hFn tree.HashFn) tree.Root {
	var a, b tree.Root
	copy(a[:], p[0:32])
	copy(b[:], p[32:48])
	return hFn(a, b)
}

func (p KZGCommitment) MarshalText() ([]byte, error) {
	return []byte("0x" + hex.EncodeToString(p[:])), nil
}

func (p KZGCommitment) String() string {
	return "0x" + hex.EncodeToString(p[:])
}

func (p *KZGCommitment) UnmarshalText(text []byte) error {
	return hexutil.UnmarshalFixedText("KZGCommitment", text, p[:])
}

func (p *KZGCommitment) Point() (*bls.G1Point, error) {
	return bls.FromCompressedG1(p[:])
}

func (kzg KZGCommitment) ComputeVersionedHash() common.Hash {
	h := crypto.Keccak256Hash(kzg[:])
	h[0] = params.BlobCommitmentVersionKZG
	return h
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

func (p KZGProof) HashTreeRoot(hFn tree.HashFn) tree.Root {
	var a, b tree.Root
	copy(a[:], p[0:32])
	copy(b[:], p[32:48])
	return hFn(a, b)
}

func (p KZGProof) MarshalText() ([]byte, error) {
	return []byte("0x" + hex.EncodeToString(p[:])), nil
}

func (p KZGProof) String() string {
	return "0x" + hex.EncodeToString(p[:])
}

func (p *KZGProof) UnmarshalText(text []byte) error {
	return hexutil.UnmarshalFixedText("KZGProof", text, p[:])
}

type BLSFieldElement [32]byte

func (p BLSFieldElement) MarshalText() ([]byte, error) {
	return []byte("0x" + hex.EncodeToString(p[:])), nil
}

func (p BLSFieldElement) String() string {
	return "0x" + hex.EncodeToString(p[:])
}

func (p *BLSFieldElement) UnmarshalText(text []byte) error {
	return hexutil.UnmarshalFixedText("BLSFieldElement", text, p[:])
}

// Blob data
type Blob [params.FieldElementsPerBlob]BLSFieldElement

// kzg.Blob interface
func (blob Blob) Len() int {
	return len(blob)
}

// kzg.Blob interface
func (blob Blob) At(i int) [32]byte {
	return [32]byte(blob[i])
}

func (blob *Blob) Deserialize(dr *codec.DecodingReader) error {
	if blob == nil {
		return errors.New("cannot decode ssz into nil Blob")
	}
	for i := uint64(0); i < params.FieldElementsPerBlob; i++ {
		// TODO: do we want to check if each field element is within range?
		if _, err := dr.Read(blob[i][:]); err != nil {
			return err
		}
	}
	return nil
}

func (blob *Blob) Serialize(w *codec.EncodingWriter) error {
	for i := range blob {
		if err := w.Write(blob[i][:]); err != nil {
			return err
		}
	}
	return nil
}

func (blob *Blob) ByteLength() (out uint64) {
	return params.FieldElementsPerBlob * 32
}

func (blob *Blob) FixedLength() uint64 {
	return params.FieldElementsPerBlob * 32
}

func (blob *Blob) HashTreeRoot(hFn tree.HashFn) tree.Root {
	return hFn.ComplexVectorHTR(func(i uint64) tree.HTR {
		return (*tree.Root)(&blob[i])
	}, params.FieldElementsPerBlob)
}

func (blob *Blob) MarshalText() ([]byte, error) {
	out := make([]byte, 2+params.FieldElementsPerBlob*32*2)
	copy(out[:2], "0x")
	j := 2
	for _, elem := range blob {
		hex.Encode(out[j:j+64], elem[:])
		j += 64
	}
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
	j := 0
	for i := 2; i < l; i += 64 {
		if _, err := hex.Decode(blob[j][:], text[i:i+64]); err != nil {
			return fmt.Errorf("blob item %d is not formatted correctly: %v", j, err)
		}
		j += 1
	}
	return nil
}

// Parse blob into Fr elements array
func (blob *Blob) Parse() (out []bls.Fr, err error) {
	out = make([]bls.Fr, params.FieldElementsPerBlob)
	for i, chunk := range blob {
		ok := bls.FrFrom32(&out[i], chunk)
		if !ok {
			return nil, errors.New("internal error commitments")
		}
	}
	return out, nil
}

type BlobKzgs []KZGCommitment

// kzg.KZGCommitmentSequence interface
func (bk BlobKzgs) Len() int {
	return len(bk)
}

func (bk BlobKzgs) At(i int) kzg.KZGCommitment {
	return kzg.KZGCommitment(bk[i])
}

// Extract the crypto material underlying these commitments
func (li BlobKzgs) Parse() ([]*bls.G1Point, error) {
	out := make([]*bls.G1Point, len(li))
	for i, c := range li {
		p, err := c.Point()
		if err != nil {
			return nil, fmt.Errorf("failed to parse commitment %d: %v", i, err)
		}
		out[i] = p
	}
	return out, nil
}

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

func (li *BlobKzgs) FixedLength() uint64 {
	return 0
}

func (li BlobKzgs) HashTreeRoot(hFn tree.HashFn) tree.Root {
	return hFn.ComplexListHTR(func(i uint64) tree.HTR {
		return &li[i]
	}, uint64(len(li)), params.MaxBlobsPerBlock)
}

func (li BlobKzgs) copy() BlobKzgs {
	cpy := make(BlobKzgs, len(li))
	copy(cpy, li)
	return cpy
}

type Blobs []Blob

// kzg.BlobSequence interface
func (blobs Blobs) Len() int {
	return len(blobs)
}

// kzg.BlobSequence interface
func (blobs Blobs) At(i int) kzg.Blob {
	return blobs[i]
}

// Extract the crypto material underlying these blobs
func (blobs Blobs) Parse() ([][]bls.Fr, error) {
	out := make([][]bls.Fr, len(blobs))
	for i, b := range blobs {
		blob, err := b.Parse()
		if err != nil {
			return nil, fmt.Errorf("failed to parse blob %d: %v", i, err)
		}
		out[i] = blob
	}
	return out, nil
}

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

func (li Blobs) HashTreeRoot(hFn tree.HashFn) tree.Root {
	length := uint64(len(li))
	return hFn.ComplexListHTR(func(i uint64) tree.HTR {
		if i < length {
			return &li[i]
		}
		return nil
	}, length, params.MaxBlobsPerBlock)
}

func (blobs Blobs) copy() Blobs {
	cpy := make(Blobs, len(blobs))
	copy(cpy, blobs) // each blob element is an array and gets deep-copied
	return cpy
}

// Return KZG commitments, versioned hashes and the aggregated KZG proof that correspond to these blobs
func (blobs Blobs) ComputeCommitmentsAndAggregatedProof() (commitments []KZGCommitment, versionedHashes []common.Hash, aggregatedProof KZGProof, err error) {
	commitments = make([]KZGCommitment, len(blobs))
	versionedHashes = make([]common.Hash, len(blobs))
	for i, blob := range blobs {
		c, ok := kzg.BlobToKZGCommitment(blob)
		if !ok {
			return nil, nil, KZGProof{}, errors.New("could not convert blob to commitment")
		}
		commitments[i] = KZGCommitment(c)
		versionedHashes[i] = commitments[i].ComputeVersionedHash()
	}

	var kzgProof KZGProof
	if len(blobs) != 0 {
		proof, err := kzg.ComputeAggregateKZGProof(blobs)
		if err != nil {
			return nil, nil, KZGProof{}, err
		}
		kzgProof = KZGProof(proof)
	}

	return commitments, versionedHashes, kzgProof, nil
}

type BlobsAndCommitments struct {
	Blobs    Blobs
	BlobKzgs BlobKzgs
}

func (b *BlobsAndCommitments) HashTreeRoot(hFn tree.HashFn) tree.Root {
	return hFn.HashTreeRoot(&b.Blobs, &b.BlobKzgs)
}

func (b *BlobsAndCommitments) Serialize(w *codec.EncodingWriter) error {
	return w.Container(&b.Blobs, &b.BlobKzgs)
}

func (b *BlobsAndCommitments) ByteLength() uint64 {
	return codec.ContainerLength(&b.Blobs, &b.BlobKzgs)
}

func (b *BlobsAndCommitments) FixedLength() uint64 {
	return 0
}

type BlobTxWrapper struct {
	Tx                 SignedBlobTx
	BlobKzgs           BlobKzgs
	Blobs              Blobs
	KzgAggregatedProof KZGProof
}

func (txw *BlobTxWrapper) Deserialize(dr *codec.DecodingReader) error {
	return dr.Container(&txw.Tx, &txw.BlobKzgs, &txw.Blobs, &txw.KzgAggregatedProof)
}

func (txw *BlobTxWrapper) Serialize(w *codec.EncodingWriter) error {
	return w.Container(&txw.Tx, &txw.BlobKzgs, &txw.Blobs, &txw.KzgAggregatedProof)
}

func (txw *BlobTxWrapper) ByteLength() uint64 {
	return codec.ContainerLength(&txw.Tx, &txw.BlobKzgs, &txw.Blobs, &txw.KzgAggregatedProof)
}

func (txw *BlobTxWrapper) FixedLength() uint64 {
	return 0
}

func (txw *BlobTxWrapper) HashTreeRoot(hFn tree.HashFn) tree.Root {
	return hFn.HashTreeRoot(&txw.Tx, &txw.BlobKzgs, &txw.Blobs, &txw.KzgAggregatedProof)
}

type BlobTxWrapData struct {
	BlobKzgs           BlobKzgs
	Blobs              Blobs
	KzgAggregatedProof KZGProof
}

func (b *BlobTxWrapData) sizeWrapData() common.StorageSize {
	return common.StorageSize(4 + 4 + b.BlobKzgs.ByteLength() + b.Blobs.ByteLength() + b.KzgAggregatedProof.ByteLength())
}

// validateBlobTransactionWrapper implements validate_blob_transaction_wrapper from EIP-4844
func (b *BlobTxWrapData) validateBlobTransactionWrapper(inner Transaction) error {
	blobTx, ok := inner.(*SignedBlobTx)
	if !ok {
		return fmt.Errorf("expected signed blob tx, got %T", inner)
	}
	l1 := len(b.BlobKzgs)
	l2 := len(blobTx.Message.BlobVersionedHashes)
	l3 := len(b.Blobs)
	if l1 != l2 || l2 != l3 {
		return fmt.Errorf("lengths don't match %v %v %v", l1, l2, l3)
	}
	// the following check isn't strictly necessary as it would be caught by data gas processing
	// (and hence it is not explicitly in the spec for this function), but it doesn't hurt to fail
	// early in case we are getting spammed with too many blobs or there is a bug somewhere:
	if l1 > params.MaxBlobsPerBlock {
		return fmt.Errorf("number of blobs exceeds max: %v", l1)
	}
	ok, err := kzg.VerifyAggregateKZGProof(b.Blobs, b.BlobKzgs, kzg.KZGProof(b.KzgAggregatedProof))
	if err != nil {
		return fmt.Errorf("error during proof verification: %v", err)
	}
	if !ok {
		return errors.New("failed to verify kzg")
	}
	for i, h := range blobTx.Message.BlobVersionedHashes {
		if computed := b.BlobKzgs[i].ComputeVersionedHash(); computed != h {
			return fmt.Errorf("versioned hash %d supposedly %s but does not match computed %s", i, h, computed)
		}
	}
	return nil
}

func (b *BlobTxWrapData) copy() TxWrapData {
	return &BlobTxWrapData{
		BlobKzgs:           b.BlobKzgs.copy(),
		Blobs:              b.Blobs.copy(),
		KzgAggregatedProof: b.KzgAggregatedProof,
	}
}

func (b *BlobTxWrapData) kzgs() BlobKzgs {
	return b.BlobKzgs
}

func (b *BlobTxWrapData) blobs() Blobs {
	return b.Blobs
}

func (b *BlobTxWrapData) aggregatedProof() KZGProof {
	return b.KzgAggregatedProof
}

func (b *BlobTxWrapData) encodeTyped(w io.Writer, txdata Transaction) error {
	if _, err := w.Write([]byte{BlobTxType}); err != nil {
		return err
	}
	blobTx, ok := txdata.(*SignedBlobTx)
	if !ok {
		return fmt.Errorf("expected signed blob tx, got %T", txdata)
	}
	wrapped := BlobTxWrapper{
		Tx:                 *blobTx,
		BlobKzgs:           b.BlobKzgs,
		Blobs:              b.Blobs,
		KzgAggregatedProof: b.KzgAggregatedProof,
	}
	return EncodeSSZ(w, &wrapped)
}
