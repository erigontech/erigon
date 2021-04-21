package adapter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/petar/GoLLRB/llrb"
)

type StateReader struct {
	blockNr uint64
	tx      ethdb.Tx
}

func NewStateReader(tx ethdb.Tx, blockNr uint64) *StateReader {
	return &StateReader{
		tx:      tx,
		blockNr: blockNr,
	}
}

func (r *StateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := state.GetAsOf(r.tx, false /* storage */, address[:], r.blockNr+1)
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (r *StateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := state.GetAsOf(r.tx, true /* storage */, compositeKey, r.blockNr+1)
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (r *StateReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], crypto.Keccak256(nil)) {
		return nil, nil
	}
	var val []byte
	v, err := r.tx.GetOne(dbutils.CodeBucket, codeHash[:])
	if err != nil {
		return nil, err
	}
	val = common.CopyBytes(v)
	return val, nil
}

func (r *StateReader) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (r *StateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

func (r *StateReader) ForEachStorage(addr common.Address, startLocation common.Hash, cb func(key, seckey common.Hash, value uint256.Int) bool, maxResults int) error {
	var s [common.AddressLength + common.IncarnationLength + common.HashLength]byte
	copy(s[:], addr[:])
	accData, err := state.GetAsOf(r.tx, false /* storage */, addr[:], r.blockNr+1)
	if err != nil {
		if errors.Is(err, ethdb.ErrKeyNotFound) {
			return fmt.Errorf("account %x not found at %d", addr, r.blockNr)
		}
		return fmt.Errorf("retrieving account %x: %w", addr, err)
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(accData); err != nil {
		return fmt.Errorf("decoding account %x: %w", addr, err)
	}
	binary.BigEndian.PutUint64(s[common.AddressLength:], acc.Incarnation)
	copy(s[common.AddressLength+common.IncarnationLength:], startLocation[:])
	var lastKey common.Hash
	min := &storageItem{key: startLocation}
	st := llrb.New()
	if err := state.WalkAsOfStorage(r.tx, addr, acc.Incarnation, startLocation, r.blockNr+1, func(kAddr, kLoc, vs []byte) (bool, error) {
		if !bytes.HasPrefix(kAddr, addr[:]) {
			return false, nil
		}
		if len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		si := storageItem{}
		copy(si.key[:], kLoc)
		if st.Has(&si) {
			return true, nil
		}
		si.value.SetBytes(vs)
		st.InsertNoReplace(&si)
		if bytes.Compare(kLoc[:], lastKey[:]) > 0 {
			// Beyond overrides
			return st.Len() < maxResults, nil
		}
		return st.Len() < maxResults, nil
	}); err != nil {
		return fmt.Errorf("walk ForEachStorage: %w", err)
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	results := 0
	st.AscendGreaterOrEqual(min, func(i llrb.Item) bool {
		item := i.(*storageItem)
		if !item.value.IsZero() {
			h.Sha.Reset()
			//nolint:errcheck
			h.Sha.Write(item.key[:])
			//nolint:errcheck
			h.Sha.Read(item.seckey[:])
			cb(item.key, item.seckey, item.value)
			results++
		}
		return results < maxResults
	})
	return nil
}

type storageItem struct {
	key, seckey common.Hash
	value       uint256.Int
}

func (a *storageItem) Less(b llrb.Item) bool {
	bi := b.(*storageItem)
	return bytes.Compare(a.key[:], bi.key[:]) < 0
}
