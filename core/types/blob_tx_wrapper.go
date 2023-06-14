package types

import (
	"fmt"
	"io"
	"math/big"
	"math/bits"
	"time"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	libkzg "github.com/ledgerwatch/erigon-lib/crypto/kzg"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

const (
	LEN_BLOB = params.FieldElementsPerBlob * 32 // 131072
	LEN_48   = 48                               // KZGCommitment & KZGProof sizes
)

type KZGCommitment [LEN_48]byte // Compressed BLS12-381 G1 element
type KZGProof [LEN_48]byte
type Blob [LEN_BLOB]byte

type BlobKzgs []KZGCommitment
type KZGProofs []KZGProof
type Blobs []Blob

type BlobTxWrapper struct {
	Tx          SignedBlobTx
	Commitments BlobKzgs
	Blobs       Blobs
	Proofs      KZGProofs
}

/* Blob methods */

func (b *Blob) payloadSize() int {
	size := 1                                             // 0xb7
	size += libcommon.BitLenToByteLen(bits.Len(LEN_BLOB)) // params.FieldElementsPerBlob * 32 = 131072 (length encoding size)
	size += LEN_BLOB                                      // byte_array it self
	return size
}

/* BlobKzgs methods */

func (li BlobKzgs) copy() BlobKzgs {
	cpy := make(BlobKzgs, len(li))
	copy(cpy, li)
	return cpy
}

func (li BlobKzgs) payloadSize() int {
	size := 49 * len(li)
	if size >= 56 {
		size += libcommon.BitLenToByteLen(bits.Len(uint(size))) // BE encoding of the length of hashes
	}
	return size
}

func (li BlobKzgs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	b[0] = 128 + LEN_48
	for _, arr := range li {
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(arr[:]); err != nil {
			return err
		}
	}
	return nil
}

/* KZGProofs methods */

func (li KZGProofs) copy() KZGProofs {
	cpy := make(KZGProofs, len(li))
	copy(cpy, li)
	return cpy
}

func (li KZGProofs) payloadSize() int {
	size := 49 * len(li)
	if size >= 56 {
		size += libcommon.BitLenToByteLen(bits.Len(uint(size))) // BE encoding of the length of hashes
	}
	return size
}

func (li KZGProofs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	b[0] = 128 + LEN_48
	for _, arr := range li {
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(arr[:]); err != nil {
			return err
		}
	}
	return nil
}

/* Blobs methods */

func (blobs Blobs) copy() Blobs {
	cpy := make(Blobs, len(blobs))
	copy(cpy, blobs) // each blob element is an array and gets deep-copied
	return cpy
}

func (blobs Blobs) payloadSize() int {
	total := 0
	if len(blobs) > 0 {
		total = len(blobs) * blobs[0].payloadSize()
		total += libcommon.BitLenToByteLen(bits.Len(uint(total)))
	}
	return total
}

func (blobs Blobs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	for _, arr := range blobs {
		if err := rlp.EncodeStringSizePrefix(LEN_BLOB, w, b); err != nil {
			return err
		}
		if _, err := w.Write(arr[:]); err != nil {
			return err
		}
	}

	return nil
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

func (c KZGCommitment) ComputeVersionedHash() libcommon.Hash {
	return libcommon.Hash(libkzg.KZGToVersionedHash(gokzg4844.KZGCommitment(c)))
}

/* BlobTxWrapper methods */

// validateBlobTransactionWrapper implements validate_blob_transaction_wrapper from EIP-4844
func (txw *BlobTxWrapper) ValidateBlobTransactionWrapper() error {
	blobTx := txw.Tx
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
func (txw *BlobTxWrapper) GetFeeCap() *uint256.Int { return txw.Tx.GetFeeCap() }

func (txw *BlobTxWrapper) Cost() *uint256.Int { return txw.Tx.GetFeeCap() }

func (txw *BlobTxWrapper) GetDataHashes() []libcommon.Hash { return txw.Tx.GetDataHashes() }

func (txw *BlobTxWrapper) GetGas() uint64            { return txw.Tx.GetGas() }
func (txw *BlobTxWrapper) GetDataGas() uint64        { return txw.Tx.GetDataGas() }
func (txw *BlobTxWrapper) GetValue() *uint256.Int    { return txw.Tx.GetValue() }
func (txw *BlobTxWrapper) Time() time.Time           { return txw.Tx.Time() }
func (txw *BlobTxWrapper) GetTo() *libcommon.Address { return txw.Tx.GetTo() }

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

func (txw *BlobTxWrapper) GetData() []byte { return txw.Tx.GetData() }

func (txw *BlobTxWrapper) GetAccessList() types2.AccessList { return txw.Tx.GetAccessList() }

func (txw *BlobTxWrapper) Protected() bool { return txw.Tx.Protected() }

func (txw *BlobTxWrapper) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return txw.Tx.RawSignatureValues()
}

func (txw *BlobTxWrapper) Sender(s Signer) (libcommon.Address, error) { return txw.Tx.Sender(s) }

func (txw *BlobTxWrapper) GetSender() (libcommon.Address, bool) { return txw.Tx.GetSender() }

func (txw *BlobTxWrapper) SetSender(address libcommon.Address) { txw.Tx.SetSender(address) }

func (txw *BlobTxWrapper) IsContractDeploy() bool { return txw.Tx.IsContractDeploy() }

func (txw *BlobTxWrapper) Unwrap() Transaction { return &txw.Tx }

func (txw BlobTxWrapper) EncodingSize() int {
	txSize, commitmentsSize, proofsSize, blobsSize := txw.payloadSize()
	payloadSize := txSize + commitmentsSize + proofsSize + blobsSize
	envelopeSize := payloadSize
	// Add envelope size and type size
	if payloadSize >= 56 {
		envelopeSize += libcommon.BitLenToByteLen(bits.Len(uint(payloadSize)))
	}
	envelopeSize += 2
	return envelopeSize
}

func (txw BlobTxWrapper) payloadSize() (int, int, int, int) {
	txSize, _, _, _, _ := txw.Tx.payloadSize()
	commitmentsSize := txw.Commitments.payloadSize()
	proofsSize := txw.Proofs.payloadSize()
	blobsSize := txw.Blobs.payloadSize()
	return txSize, commitmentsSize, proofsSize, blobsSize
}

func (txw BlobTxWrapper) encodePayload(w io.Writer, b []byte, payloadSize, commitmentsSize, proofsSize, blobsSize int) error {
	// prefix, encode txw payload size
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	txPayloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := txw.Tx.payloadSize()
	if err := txw.Tx.encodePayload(w, b, txPayloadSize, nonceLen, gasLen, accessListLen, blobHashesLen); err != nil {
		return err
	}

	// TODO: encode in order (see EIP-4844 updates)
	if err := txw.Commitments.encodePayload(w, b, commitmentsSize); err != nil {
		return err
	}
	if err := txw.Blobs.encodePayload(w, b, blobsSize); err != nil {
		return err
	}
	txw.Proofs.encodePayload(w, b, proofsSize)
	return nil
}

func (txw *BlobTxWrapper) MarshalBinary(w io.Writer) error {
	return nil
}

func (txw BlobTxWrapper) EncodeRLP(w io.Writer) error {
	txSize, commitmentsSize, proofsSize, blobsSize := txw.payloadSize()
	payloadSize := txSize + commitmentsSize + proofsSize + blobsSize
	envelopeSize := payloadSize
	if payloadSize >= 56 {
		envelopeSize += libcommon.BitLenToByteLen(bits.Len(uint(payloadSize)))
	}
	// size of struct prefix and TxType
	envelopeSize += 2
	var b [33]byte
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := txw.encodePayload(w, b[:], payloadSize, commitmentsSize, proofsSize, blobsSize); err != nil {
		return err
	}
	return nil
}

func (txw BlobTxWrapper) DecodeRLP(s *rlp.Stream) error {
	// TODO
	return nil
}
