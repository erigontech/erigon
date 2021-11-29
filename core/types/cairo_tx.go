package types

import (
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rlp"
	"io"
	"math/big"
)

type CairoTransaction struct {
	CommonTx

	Tip        *uint256.Int
	FeeCap     *uint256.Int
	AccessList AccessList
}

func (tx CairoTransaction) Type() byte {
	return CairoType
}

func (tx CairoTransaction) IsStarkNet() bool {
	return true
}

func (tx *CairoTransaction) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for ChainID: %d", len(b))
	}
	//tx.ChainID = new(uint256.Int).SetBytes(b)
	if tx.Nonce, err = s.Uint(); err != nil {
		return err
	}
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for MaxPriorityFeePerGas: %d", len(b))
	}
	tx.Tip = new(uint256.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for MaxFeePerGas: %d", len(b))
	}
	tx.FeeCap = new(uint256.Int).SetBytes(b)
	if tx.Gas, err = s.Uint(); err != nil {
		return err
	}
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 0 && len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	if len(b) > 0 {
		tx.To = &common.Address{}
		copy((*tx.To)[:], b)
	}
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for Value: %d", len(b))
	}
	tx.Value = new(uint256.Int).SetBytes(b)
	if tx.Data, err = s.Bytes(); err != nil {
		return err
	}
	// decode AccessList
	tx.AccessList = AccessList{}
	if err = decodeAccessList(&tx.AccessList, s); err != nil {
		return err
	}
	// decode V
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for V: %d", len(b))
	}
	tx.V.SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for R: %d", len(b))
	}
	tx.R.SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for S: %d", len(b))
	}
	tx.S.SetBytes(b)
	return s.ListEnd()
}

func (tx CairoTransaction) GetChainID() *uint256.Int {
	panic("implement me")
}

func (tx CairoTransaction) GetPrice() *uint256.Int {
	panic("implement me")
}

func (tx CairoTransaction) GetTip() *uint256.Int {
	panic("implement me")
}

func (tx CairoTransaction) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	panic("implement me")
}

func (tx CairoTransaction) GetFeeCap() *uint256.Int {
	panic("implement me")
}

func (tx CairoTransaction) Cost() *uint256.Int {
	panic("implement me")
}

func (tx CairoTransaction) AsMessage(s Signer, baseFee *big.Int) (Message, error) {
	panic("implement me")
}

func (tx CairoTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	panic("implement me")
}

func (tx CairoTransaction) FakeSign(address common.Address) (Transaction, error) {
	panic("implement me")
}

func (tx CairoTransaction) Hash() common.Hash {
	panic("implement me")
}

func (tx CairoTransaction) SigningHash(chainID *big.Int) common.Hash {
	panic("implement me")
}

func (tx CairoTransaction) Size() common.StorageSize {
	panic("implement me")
}

func (tx CairoTransaction) GetAccessList() AccessList {
	panic("implement me")
}

func (tx CairoTransaction) Protected() bool {
	panic("implement me")
}

func (tx CairoTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	panic("implement me")
}

func (tx CairoTransaction) MarshalBinary(w io.Writer) error {
	panic("implement me")
}

func (tx CairoTransaction) Sender(signer Signer) (common.Address, error) {
	panic("implement me")
}

func (tx CairoTransaction) SetSender(address common.Address) {
	panic("implement me")
}
