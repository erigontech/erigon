package types

import (
	"bytes"
	"fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/rlp"
)

const (
	pLen = 48 // pubkey size
	wLen = 32 // withdrawalCredentials size
	sLen = 96 // signature size
)

var DepositRequestType byte = 0

//go:generate go run github.com/fjl/gencodec -type Deposit -field-override depositMarshaling -out gen_deposit_json.go

type Deposit struct {
	Pubkey                [pLen]byte
	WithdrawalCredentials libcommon.Hash
	Amount                uint64
	Signature             [sLen]byte
	Index                 uint64
}

func (d *Deposit) EncodingSize() (encodingSize int) {
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(d.Amount)

	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(d.Index)

	encodingSize += 180 // 1 + 48 + 1 + 32 + 1 + 1 + 96 (0x80 + pLen, 0x80 + wLen, 0xb8 + 1 + sLen)
	return encodingSize
}

func (d *Deposit) EncodeRLP(w io.Writer) error {
	encodingSize := d.EncodingSize()

	var b [33]byte
	if err := EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}

	b[0] = 0x80 + pLen
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(d.Pubkey[:]); err != nil {
		return err
	}

	b[0] = 0x80 + wLen
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(d.WithdrawalCredentials[:]); err != nil {
		return err
	}

	if err := rlp.EncodeInt(d.Amount, w, b[:]); err != nil {
		return err
	}

	b[0] = 0xb8
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	b[0] = sLen
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(d.Signature[:]); err != nil {
		return err
	}

	return rlp.EncodeInt(d.Index, w, b[:])
}

func (d *Deposit) DecodeRLP(s *rlp.Stream) error {

	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("deposit: pubkey decode error: %w", err)
	}
	if len(b) != pLen {
		return fmt.Errorf("deposit: pubkey error: expected length is %d, got: %d", pLen, len(b))
	}
	copy(d.Pubkey[:], b)

	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("deposit: withdrawalCridentials decode error: %w", err)
	}
	if len(b) != wLen {
		return fmt.Errorf("deposit: withdrawalCridentials error: expected length is %d, got: %d", pLen, len(b))
	}
	copy(d.WithdrawalCredentials[:], b)

	if d.Amount, err = s.Uint(); err != nil {
		return fmt.Errorf("deposit: amount error:: %w", err)
	}

	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("deposit: signature decode error: %w", err)
	}
	if len(b) != sLen {
		return fmt.Errorf("deposit: signature error: expected length is %d, got: %d", pLen, len(b))
	}
	copy(d.Signature[:], b)

	if d.Index, err = s.Uint(); err != nil {
		return fmt.Errorf("deposit: amount error:: %w", err)
	}

	return s.ListEnd()
}

func (*Deposit) Clone() clonable.Clonable { // TODO(racytech): make sure this one is correct
	return &Deposit{}
}

// field type overrides for gencodec
type depositMarshaling struct {
	Pubkey                hexutility.Bytes
	WithdrawalCredentials hexutility.Bytes
	Amount                hexutil.Uint64
	Signature             hexutility.Bytes
	Index                 hexutil.Uint64
}

// Deposits implements DerivableList for deposits.
type Deposits []*Deposit

func (s Deposits) Len() int { return len(s) }

// EncodeIndex encodes the i'th deposit to w. Note that this does not check for errors
// because we assume that *Deposit will only ever contain valid deposits that were either
// constructed by decoding or via public API in this package.
func (s Deposits) EncodeIndex(i int, w *bytes.Buffer) {
	rlp.Encode(w, s[i])
}

func ComputeTrieRootFromIndexedData(data Deposits) libcommon.Hash {
	return DeriveSha(data)
}
