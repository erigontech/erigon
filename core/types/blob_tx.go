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

	"github.com/ledgerwatch/erigon/rlp"
)

type BlobTx struct {
	DynamicFeeTransaction
	MaxFeePerDataGas    *uint256.Int
	BlobVersionedHashes []libcommon.Hash
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (stx BlobTx) copy() *BlobTx {
	cpy := &BlobTx{
		DynamicFeeTransaction: *stx.DynamicFeeTransaction.copy(),
		MaxFeePerDataGas:      new(uint256.Int),
		BlobVersionedHashes:   make([]libcommon.Hash, len(stx.BlobVersionedHashes)),
	}
	copy(cpy.BlobVersionedHashes, stx.BlobVersionedHashes)
	if stx.MaxFeePerDataGas != nil {
		cpy.MaxFeePerDataGas.Set(stx.MaxFeePerDataGas)
	}
	return cpy
}

func (stx BlobTx) Type() byte { return BlobTxType }

func (stx BlobTx) GetDataHashes() []libcommon.Hash {
	return stx.BlobVersionedHashes
}

func (stx BlobTx) GetDataGas() uint64 {
	return chain.DataGasPerBlob * uint64(len(stx.BlobVersionedHashes))
}

func (stx BlobTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg, err := stx.DynamicFeeTransaction.AsMessage(s, baseFee, rules)
	if err != nil {
		return Message{}, err
	}
	msg.dataHashes = stx.BlobVersionedHashes
	return msg, err
}

func (stx BlobTx) Hash() libcommon.Hash {
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

func (stx BlobTx) SigningHash(chainID *big.Int) libcommon.Hash {
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

func (stx BlobTx) payloadSize() (payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) {
	payloadSize, nonceLen, gasLen, accessListLen = stx.DynamicFeeTransaction.payloadSize()
	// size of MaxFeePerDataGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.MaxFeePerDataGas)
	// size of BlobVersionedHashes
	payloadSize++
	blobHashesLen = blobVersionedHashesSize(stx.BlobVersionedHashes)
	if blobHashesLen >= 56 {
		payloadSize += libcommon.BitLenToByteLen(bits.Len(uint(blobHashesLen)))
	}
	payloadSize += blobHashesLen
	return
}

func blobVersionedHashesSize(hashes []libcommon.Hash) int {
	return 33 * len(hashes)
}

func encodeBlobVersionedHashes(hashes []libcommon.Hash, w io.Writer, b []byte) error {
	for _, h := range hashes {
		if err := rlp.EncodeString(h[:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (stx BlobTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) error {
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
	if err := stx.Tip.EncodeRLP(w); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := stx.FeeCap.EncodeRLP(w); err != nil {
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
	if err := encodeBlobVersionedHashes(stx.BlobVersionedHashes, w, b); err != nil {
		return err
	}
	// encode y_parity
	if err := stx.V.EncodeRLP(w); err != nil {
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

func (stx BlobTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
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
	if err := stx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (stx BlobTx) MarshalBinary(w io.Writer) error {
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

func (stx *BlobTx) DecodeRLP(s *rlp.Stream) error {
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
	stx.Tip = new(uint256.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.FeeCap = new(uint256.Int).SetBytes(b)

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
	if err = decodeBlobVersionedHashes(&stx.BlobVersionedHashes, s); err != nil {
		return err
	}

	// decode V
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.V.SetBytes(b)

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

func decodeBlobVersionedHashes(hashes *[]libcommon.Hash, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobVersionedHashes: %w", err)
	}
	var b []byte
	_hash := libcommon.Hash{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == 32 {
			copy((_hash)[:], b)
			*hashes = append(*hashes, _hash)
		} else {
			return fmt.Errorf("wrong size for blobVersionedHashes: %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobVersionedHashes: %w", err)
	}

	return nil
}
