package state

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type StateWriterV4 struct {
	*state.SharedDomains
}

func WrapStateIO(s *state.SharedDomains) (*StateWriterV4, *StateReaderV4) {
	w, r := &StateWriterV4{s}, &StateReaderV4{s}
	return w, r
}

func (r *StateWriterV4) SetTxNum(txNum uint64) { r.SharedDomains.SetTxNum(txNum) }

func (w *StateWriterV4) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	return w.SharedDomains.UpdateAccountData(address.Bytes(), accounts.SerialiseV3(account), accounts.SerialiseV3(original))
}

func (w *StateWriterV4) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	//addressBytes, codeHashBytes := address.Bytes(), codeHash.Bytes()
	//fmt.Printf("code [%x] => [%x] CodeHash: %x, txNum: %d\n", address, code, codeHash, w.txNum)
	return w.SharedDomains.UpdateAccountCode(address.Bytes(), code, nil)
}

func (w *StateWriterV4) DeleteAccount(address common.Address, original *accounts.Account) error {
	addressBytes := address.Bytes()
	return w.SharedDomains.DeleteAccount(addressBytes, accounts.SerialiseV3(original))
}

func (w *StateWriterV4) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if original.Eq(value) {
		return nil
	}
	//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, v, w.txNum)
	return w.SharedDomains.WriteAccountStorage(address.Bytes(), key.Bytes(), value.Bytes(), original.Bytes())
}

func (w *StateWriterV4) CreateContract(address common.Address) error { return nil }
func (w *StateWriterV4) WriteChangeSets() error                      { return nil }
func (w *StateWriterV4) WriteHistory() error                         { return nil }

type StateReaderV4 struct {
	*state.SharedDomains
}

func (s *StateReaderV4) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := s.LatestAccount(address.Bytes())
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err := accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *StateReaderV4) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	enc, err := s.LatestStorage(address.Bytes(), key.Bytes())
	if err != nil {
		return nil, err
	}
	if enc == nil {
		return nil, nil
	}
	if len(enc) == 1 && enc[0] == 0 {
		return nil, nil
	}
	return enc, nil
}

func (s *StateReaderV4) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	return s.LatestCode(address.Bytes())
}

func (s *StateReaderV4) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	c, err := s.ReadAccountCode(address, incarnation, codeHash)
	if err != nil {
		return 0, err
	}
	return len(c), nil
}

func (s *StateReaderV4) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

type MultiStateWriter struct {
	writers []StateWriter
}

func NewMultiStateWriter(w ...StateWriter) *MultiStateWriter {
	return &MultiStateWriter{
		writers: w,
	}
}

func (m *MultiStateWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	for i, w := range m.writers {
		if err := w.UpdateAccountData(address, original, account); err != nil {
			return fmt.Errorf("%T at pos %d: UpdateAccountData: %w", w, i, err)
		}
	}
	return nil
}

func (m *MultiStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	for i, w := range m.writers {
		if err := w.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
			return fmt.Errorf("%T at pos %d: UpdateAccountCode: %w", w, i, err)
		}
	}
	return nil
}

func (m *MultiStateWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	for i, w := range m.writers {
		if err := w.DeleteAccount(address, original); err != nil {
			return fmt.Errorf("%T at pos %d: DeleteAccount: %w", w, i, err)
		}
	}
	return nil
}

func (m *MultiStateWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	for i, w := range m.writers {
		if err := w.WriteAccountStorage(address, incarnation, key, original, value); err != nil {
			return fmt.Errorf("%T at pos %d: WriteAccountStorage: %w", w, i, err)
		}
	}
	return nil
}

func (m *MultiStateWriter) CreateContract(address common.Address) error {
	for i, w := range m.writers {
		if err := w.CreateContract(address); err != nil {
			return fmt.Errorf("%T at pos %d: CreateContract: %w", w, i, err)
		}
	}
	return nil
}

type MultiStateReader struct {
	readers []StateReader
	compare bool // use first read as ethalon value for current read iteration
}

func NewMultiStateReader(compare bool, r ...StateReader) *MultiStateReader {
	return &MultiStateReader{readers: r, compare: compare}
}
func (m *MultiStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var vo accounts.Account
	var isnil bool
	for i, r := range m.readers {
		v, err := r.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			if v == nil {
				isnil = true
				continue
			}
			vo = *v
		}

		if !m.compare {
			continue
		}
		if isnil {
			if v != nil {
				log.Warn("state read invalid",
					"reader", fmt.Sprintf("%d %T", i, r), "addr", address.String(),
					"m", "nil expected, got something")

			} else {
				continue
			}
		}
		buf := new(strings.Builder)
		if vo.Nonce != v.Nonce {
			buf.WriteString(fmt.Sprintf("nonce exp: %d, %d", vo.Nonce, v.Nonce))
		}
		if !bytes.Equal(vo.CodeHash[:], v.CodeHash[:]) {
			buf.WriteString(fmt.Sprintf("code exp: %x, %x", vo.CodeHash[:], v.CodeHash[:]))
		}
		if !vo.Balance.Eq(&v.Balance) {
			buf.WriteString(fmt.Sprintf("bal exp: %v, %v", vo.Balance.String(), v.Balance.String()))
		}
		if !bytes.Equal(vo.Root[:], v.Root[:]) {
			buf.WriteString(fmt.Sprintf("root exp: %x, %x", vo.Root[:], v.Root[:]))
		}
		if buf.Len() > 0 {
			log.Warn("state read invalid",
				"reader", fmt.Sprintf("%d %T", i, r), "addr", address.String(),
				"m", buf.String())
		}
	}
	return &vo, nil
}

func (m *MultiStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	var so []byte
	for i, r := range m.readers {
		s, err := r.ReadAccountStorage(address, incarnation, key)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			so = common.Copy(s)
		}
		if !m.compare {
			continue
		}
		if !bytes.Equal(so, s) {
			log.Warn("state storage invalid read",
				"reader", fmt.Sprintf("%d %T", i, r),
				"addr", address.String(), "loc", key.String(), "expected", so, "got", s)
		}
	}
	return so, nil
}

func (m *MultiStateReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	var so []byte
	for i, r := range m.readers {
		s, err := r.ReadAccountCode(address, incarnation, codeHash)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			so = common.Copy(s)
		}
		if !m.compare {
			continue
		}
		if !bytes.Equal(so, s) {
			log.Warn("state code invalid read",
				"reader", fmt.Sprintf("%d %T", i, r),
				"addr", address.String(), "expected", so, "got", s)
		}
	}
	return so, nil
}

func (m *MultiStateReader) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	var so int
	for i, r := range m.readers {
		s, err := r.ReadAccountCodeSize(address, incarnation, codeHash)
		if err != nil {
			return 0, err
		}
		if i == 0 {
			so = s
		}
		if !m.compare {
			continue
		}
		if so != s {
			log.Warn("state code size invalid read",
				"reader", fmt.Sprintf("%d %T", i, r),
				"addr", address.String(), "expected", so, "got", s)
		}
	}
	return so, nil
}

func (m *MultiStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	var so uint64
	for i, r := range m.readers {
		s, err := r.ReadAccountIncarnation(address)
		if err != nil {
			return 0, err
		}
		if i == 0 {
			so = s
		}
		if !m.compare {
			continue
		}
		if so != s {
			log.Warn("state incarnation invalid read",
				"reader", fmt.Sprintf("%d %T", i, r),
				"addr", address.String(), "expected", so, "got", s)
		}
	}
	return so, nil
}
