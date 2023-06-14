package types

import (
	"fmt"
	"io"
	"math/big"
	"math/bits"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

type SignedBlobTx struct {
	DynamicFeeTransaction
	MaxFeePerDataGas    *uint256.Int
	BlobVersionedHashes []libcommon.Hash
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (stx SignedBlobTx) copy() *SignedBlobTx {
	cpy := &SignedBlobTx{
		DynamicFeeTransaction: *stx.DynamicFeeTransaction.copy(),
		BlobVersionedHashes:   make([]libcommon.Hash, len(stx.BlobVersionedHashes)),
	}
	copy(cpy.BlobVersionedHashes, stx.BlobVersionedHashes)
	if stx.MaxFeePerDataGas != nil {
		cpy.MaxFeePerDataGas.Set(stx.MaxFeePerDataGas)
	}
	return cpy
}

func (stx SignedBlobTx) Type() byte { return BlobTxType }

func (stx SignedBlobTx) GetDataHashes() []libcommon.Hash {
	return stx.BlobVersionedHashes
}

func (stx SignedBlobTx) GetDataGas() uint64 {
	return params.DataGasPerBlob * uint64(len(stx.BlobVersionedHashes))
}

func (stx SignedBlobTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg, err := stx.DynamicFeeTransaction.AsMessage(s, baseFee, rules)
	if err != nil {
		return Message{}, err
	}
	msg.dataHashes = stx.BlobVersionedHashes
	return msg, err
}

func (stx SignedBlobTx) Hash() libcommon.Hash {
	if hash := stx.hash.Load(); hash != nil {
		return *hash.(*libcommon.Hash)
	}
	hash := prefixedRlpHash(BlobTxType, []interface{}{
		stx.ChainID,
		stx.Nonce,
		stx.Tip,
		stx.FeeCap,
		stx.Gas,
		stx.To,
		stx.Value,
		stx.Data,
		stx.AccessList,
		stx.MaxFeePerDataGas,
		stx.BlobVersionedHashes,
		stx.V, stx.R, stx.S,
	})
	stx.hash.Store(&hash)
	return hash
}

func (stx SignedBlobTx) SigningHash(chainID *big.Int) libcommon.Hash {
	return prefixedRlpHash(
		BlobTxType,
		[]interface{}{
			chainID,
			stx.Nonce,
			stx.Tip,
			stx.FeeCap,
			stx.Gas,
			stx.To,
			stx.Value,
			stx.Data,
			stx.AccessList,
			stx.MaxFeePerDataGas,
			stx.BlobVersionedHashes,
		})
}

func (stx SignedBlobTx) payloadSize() (payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) {
	payloadSize, nonceLen, gasLen, accessListLen = stx.DynamicFeeTransaction.payloadSize()
	// size of MaxFeePerDataGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.MaxFeePerDataGas)
	// size of BlobVersionedHashes
	payloadSize++
	blobHashesLen = blobVersionedHashesSize(stx.BlobVersionedHashes)
	if blobHashesLen >= 56 {
		payloadSize += (bits.Len(uint(blobHashesLen)) + 7) / 8
	}
	payloadSize += blobHashesLen
	return
}

func blobVersionedHashesSize(hashes []libcommon.Hash) int {
	return 33 * len(hashes)
}

func encodeBloblVersionedHashes(hashes []libcommon.Hash, w io.Writer, b []byte) error {
	b[0] = 128 + 32
	for _, h := range hashes {
		// if _, err := w.Write(b[:1]); err != nil {
		// 	return err
		// }
		// if _, err := w.Write(h.Bytes()); err != nil {
		// 	return err
		// }
		if err := rlp.EncodeString(h[:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (stx SignedBlobTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := stx.ChainID.EncodeRLP(w); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeInt(stx.Nonce, w, b); err != nil {
		return err
	}
	// encode MaxPriorityFeePerGas
	if err := stx.MaxPriorityFeePerGas.EncodeRLP(w); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := stx.MaxFeePerGas.EncodeRLP(w); err != nil {
		return err
	}
	// encode Gas
	if err := rlp.EncodeInt(stx.Gas, w, b); err != nil {
		return err
	}
	// encode To
	if stx.To == nil {
		b[0] = 128
	} else {
		b[0] = 128 + 20
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if stx.To != nil {
		if _, err := w.Write(stx.To.Bytes()); err != nil {
			return err
		}
	}
	// encode Value
	if err := stx.Value.EncodeRLP(w); err != nil {
		return err
	}
	// encode Data
	if err := rlp.EncodeString(stx.Data, w, b); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(stx.AccessList, w, b); err != nil {
		return err
	}
	// encode MaxFeePerDataGas
	if err := stx.MaxFeePerDataGas.EncodeRLP(w); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(blobHashesLen, w, b); err != nil {
		return err
	}
	// encode BlobVersionedHashes
	if err := encodeBloblVersionedHashes(stx.BlobVersionedHashes, w, b); err != nil {
		return err
	}
	// encode y_parity
	if stx.YParity {
		b[0] = 1
	} else {
		b[0] = 0
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	// encode R
	if err := stx.R.EncodeRLP(w); err != nil {
		return err
	}
	// encode S
	if err := stx.S.EncodeRLP(w); err != nil {
		return err
	}
	return nil
}

func (stx SignedBlobTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
	envelopeSize := payloadSize
	if payloadSize >= 56 {
		envelopeSize += (bits.Len(uint(payloadSize)) + 7) / 8
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
	if err := stx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (stx SignedBlobTx) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
	var b [33]byte
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := stx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (stx *SignedBlobTx) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.ChainID = new(uint256.Int).SetBytes(b)

	if stx.Nonce, err = s.Uint(); err != nil {
		return err
	}

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.MaxPriorityFeePerGas = new(uint256.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.MaxFeePerGas = new(uint256.Int).SetBytes(b)

	if stx.Gas, err = s.Uint(); err != nil {
		return err
	}

	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 0 && len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	if len(b) > 0 {
		stx.To = &libcommon.Address{}
		copy((*stx.To)[:], b)
	}

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.Value = new(uint256.Int).SetBytes(b)

	if stx.Data, err = s.Bytes(); err != nil {
		return err
	}
	// decode AccessList
	stx.AccessList = types2.AccessList{}
	if err = decodeAccessList(&stx.AccessList, s); err != nil {
		return err
	}

	// decode MaxFeePerDataGas
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.MaxFeePerDataGas = new(uint256.Int).SetBytes(b)

	// decode BlobVersionedHashes
	stx.BlobVersionedHashes = []libcommon.Hash{}
	if err = decodeBlobVersionedHashes(stx.BlobVersionedHashes, s); err != nil {
		return err
	}

	// decode y_parity
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 1 {
		return fmt.Errorf("wrong size for y_parity: %d", len(b))
	}
	if b[0] == 1 {
		stx.YParity = true
	} else if b[0] == 0 {
		stx.YParity = false
	} else {
		return fmt.Errorf("wrong value for y_parity: %d", b)
	}

	// decode R
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.R.SetBytes(b)

	// decode S
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.S.SetBytes(b)
	return s.ListEnd()
}

func decodeBlobVersionedHashes(hashes []libcommon.Hash, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobVersionedHashes: %w", err)
	}
	var b []byte
	_hash := libcommon.Hash{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == 32 {
			copy((_hash)[:], b)
			hashes = append(hashes, _hash)
		} else {
			return fmt.Errorf("wrong size for blobVersionedHashes: %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobVersionedHashes: %w", err)
	}

	return nil
}
