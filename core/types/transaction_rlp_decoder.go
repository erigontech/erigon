package types

import (
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rlp"
)

type TransactionRLPDecoder struct {
	s *rlp.Stream
}

func (d *TransactionRLPDecoder) Start() error {
	if _, err := d.s.List(); err != nil {
		return err
	}

	return nil
}

func (d *TransactionRLPDecoder) End() error {
	return d.s.ListEnd()
}

func (d TransactionRLPDecoder) ChainID() (*uint256.Int, error) {
	return d.uint256Bytes()
}

func (d TransactionRLPDecoder) Nonce() (uint64, error) {
	return d.uint64()
}

func (d TransactionRLPDecoder) Tip() (*uint256.Int, error) {
	return d.uint256Bytes()
}

func (d TransactionRLPDecoder) FeeCap() (*uint256.Int, error) {
	return d.uint256Bytes()
}

func (d TransactionRLPDecoder) Gas() (uint64, error) {
	return d.uint64()
}

func (d TransactionRLPDecoder) To() (*common.Address, error) {
	b, err := d.s.Bytes()

	if err != nil {
		return nil, err
	}

	if len(b) > 0 && len(b) != 20 {
		return nil, fmt.Errorf("wrong size for To: %d", len(b))
	}

	to := &common.Address{}
	copy((*to)[:], b)

	return to, nil
}

func (d TransactionRLPDecoder) Value() (*uint256.Int, error) {
	return d.uint256Bytes()
}

func (d TransactionRLPDecoder) Data(data *[]byte) error {
	var err error
	if *data, err = d.bytes(); err != nil {
		return err
	}
	return nil
}

func (d TransactionRLPDecoder) AccessList(al *AccessList) error {
	return decodeAccessList(al, d.s)
}

func (d TransactionRLPDecoder) V() (*uint256.Int, error) {
	return d.uint256Bytes()
}

func (d TransactionRLPDecoder) R() (*uint256.Int, error) {
	return d.uint256Bytes()
}

func (d TransactionRLPDecoder) S() (*uint256.Int, error) {
	return d.uint256Bytes()
}

func (d TransactionRLPDecoder) uint64() (uint64, error) {
	value, err := d.s.Uint()

	if err != nil {
		return value, err
	}

	return value, nil
}

func (d TransactionRLPDecoder) uint256Bytes() (*uint256.Int, error) {
	b, err := d.s.Uint256Bytes()

	if err != nil {
		return nil, err
	}

	return new(uint256.Int).SetBytes(b), nil
}

func (d TransactionRLPDecoder) bytes() ([]byte, error) {
	b, err := d.s.Bytes()

	if err != nil {
		return nil, err
	}

	return b, nil
}
