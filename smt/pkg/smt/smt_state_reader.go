package smt

import (
	"bytes"
	"context"
	"errors"
	"math/big"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

var _ state.StateReader = (*SMT)(nil)

// ReadAccountData reads account data from the SMT
func (s *SMT) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	balance, err := s.GetAccountBalance(address)
	if err != nil {
		return nil, err
	}

	nonce, err := s.GetAccountNonce(address)
	if err != nil {
		return nil, err
	}

	codeHash, err := s.GetAccountCodeHash(address)
	if err != nil {
		return nil, err
	}

	account := &accounts.Account{
		Balance:  *balance,
		Nonce:    nonce.Uint64(),
		CodeHash: codeHash,
		Root:     libcommon.Hash{},
	}

	return account, nil
}

// ReadAccountStorage reads account storage from the SMT (not implemented for SMT)
func (s *SMT) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	value, err := s.getValue(0, address, key)
	if err != nil {
		return []byte{}, err
	}

	return value, nil
}

// ReadAccountCode reads account code from the SMT
func (s *SMT) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	code, err := s.Db.GetCode(codeHash.Bytes())
	if err != nil {
		return []byte{}, err
	}

	return code, nil
}

// ReadAccountCodeSize reads account code size from the SMT
func (s *SMT) ReadAccountCodeSize(address libcommon.Address, _ uint64, _ libcommon.Hash) (int, error) {
	valueInBytes, err := s.getValue(utils.SC_LENGTH, address, nil)
	if err != nil {
		return 0, err
	}

	sizeBig := big.NewInt(0).SetBytes(valueInBytes)

	if !sizeBig.IsInt64() {
		err = errors.New("code size value is too large to fit into an int")
		return 0, err
	}

	sizeInt64 := sizeBig.Int64()
	if sizeInt64 > int64(^uint(0)>>1) {
		err = errors.New("code size value overflows int")
		log.Error("failed to get account code size", "error", err)
		return 0, err
	}

	return int(sizeInt64), nil
}

// ReadAccountIncarnation reads account incarnation from the SMT (not implemented for SMT)
func (s *SMT) ReadAccountIncarnation(_ libcommon.Address) (uint64, error) {
	return 0, errors.New("ReadAccountIncarnation not implemented for SMT")
}

// GetAccountBalance returns the balance of an account from the SMT
func (s *SMT) GetAccountBalance(address libcommon.Address) (*uint256.Int, error) {
	valueInBytes, err := s.getValue(utils.KEY_BALANCE, address, nil)
	if err != nil {
		log.Error("failed to get balance", "error", err)
		return nil, err
	}

	balance := uint256.NewInt(0).SetBytes(valueInBytes)

	return balance, nil
}

// GetAccountNonce returns the nonce of an account from the SMT
func (s *SMT) GetAccountNonce(address libcommon.Address) (*uint256.Int, error) {
	valueInBytes, err := s.getValue(utils.KEY_NONCE, address, nil)
	if err != nil {
		log.Error("failed to get nonce", "error", err)
		return nil, err
	}

	nonce := uint256.NewInt(0).SetBytes(valueInBytes)

	return nonce, nil
}

// GetAccountCodeHash returns the code hash of an account from the SMT
func (s *SMT) GetAccountCodeHash(address libcommon.Address) (libcommon.Hash, error) {
	valueInBytes, err := s.getValue(utils.SC_CODE, address, nil)
	if err != nil {
		log.Error("failed to get code hash", "error", err)
		return libcommon.Hash{}, err
	}

	codeHash := libcommon.Hash{}
	codeHash.SetBytes(valueInBytes)

	return codeHash, nil
}

// getValue returns the value of a key from SMT by traversing the SMT
func (s *SMT) getValue(key int, address libcommon.Address, storageKey *libcommon.Hash) ([]byte, error) {
	var kn utils.NodeKey

	if storageKey == nil {
		kn = utils.Key(address.String(), key)
	} else {
		a := utils.ConvertHexToBigInt(address.String())
		add := utils.ScalarToArrayBig(a)

		kn = utils.KeyContractStorage(add, storageKey.String())
	}

	return s.getValueInBytes(kn)
}

// getValueInBytes returns the value of a key from SMT in bytes by traversing the SMT
func (s *SMT) getValueInBytes(nodeKey utils.NodeKey) ([]byte, error) {
	value := []byte{}

	keyPath := nodeKey.GetPath()

	keyPathBytes := make([]byte, len(keyPath))
	for i, k := range keyPath {
		keyPathBytes[i] = byte(k)
	}

	action := func(prefix []byte, _ utils.NodeKey, v utils.NodeValue12) (bool, error) {
		if !bytes.HasPrefix(keyPathBytes, prefix) {
			return false, nil
		}

		if v.IsFinalNode() {
			valHash := v.Get4to8()
			v, err := s.Db.Get(*valHash)
			if err != nil {
				return false, err
			}
			vInBytes := utils.ArrayBigToScalar(utils.BigIntArrayFromNodeValue8(v.GetNodeValue8())).Bytes()

			value = vInBytes
			return false, nil
		}

		return true, nil
	}

	root, err := s.Db.GetLastRoot()
	if err != nil {
		return nil, err
	}

	err = s.Traverse(context.Background(), root, action)
	if err != nil {
		return nil, err
	}

	return value, nil
}
