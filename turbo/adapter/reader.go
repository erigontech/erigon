package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/petar/GoLLRB/llrb"
)

type StateReader struct {
	accountReads map[common.Address]struct{}
	storageReads map[common.Address]map[common.Hash]struct{}
	codeReads    map[common.Address]struct{}
	blockNr      uint64
	tx           ethdb.Tx
	storage      map[common.Address]*llrb.LLRB
}

func NewStateReader(tx ethdb.Tx, blockNr uint64) *StateReader {
	return &StateReader{
		accountReads: make(map[common.Address]struct{}),
		storageReads: make(map[common.Address]map[common.Hash]struct{}),
		codeReads:    make(map[common.Address]struct{}),
		tx:           tx,
		blockNr:      blockNr,
		storage:      make(map[common.Address]*llrb.LLRB),
	}
}

func (r *StateReader) GetAccountReads() [][]byte {
	output := make([][]byte, 0)
	for address := range r.accountReads {
		output = append(output, address.Bytes())
	}
	return output
}

func (r *StateReader) GetStorageReads() [][]byte {
	output := make([][]byte, 0)
	for address, m := range r.storageReads {
		for key := range m {
			output = append(output, append(address.Bytes(), key.Bytes()...))
		}
	}
	return output
}

func (r *StateReader) GetCodeReads() [][]byte {
	output := make([][]byte, 0)
	for key := range r.codeReads {
		output = append(output, key.Bytes())
	}
	return output
}

func (r *StateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	r.accountReads[address] = struct{}{}
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
	m, ok := r.storageReads[address]
	if !ok {
		m = make(map[common.Hash]struct{})
		r.storageReads[address] = m
	}
	m[*key] = struct{}{}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key)
	enc, err := state.GetAsOf(r.tx, true /* storage */, compositeKey, r.blockNr+1)
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (r *StateReader) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	r.codeReads[address] = struct{}{}
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

func (r *StateReader) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (r *StateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

func (r *StateReader) UpdateAccountData(_ context.Context, address common.Address, original, account *accounts.Account) error {
	return nil
}

func (r *StateReader) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	return nil
}

func (r *StateReader) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (r *StateReader) WriteAccountStorage(_ context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	t, ok := r.storage[address]
	if !ok {
		t = llrb.New()
		r.storage[address] = t
	}
	i := &storageItem{key: *key, value: *value}
	t.ReplaceOrInsert(i)
	return nil
}

func (r *StateReader) CreateContract(address common.Address) error {
	delete(r.storage, address)
	return nil
}

func (r *StateReader) ForEachStorage(addr common.Address, start []byte, cb func(key, seckey common.Hash, value uint256.Int) bool, maxResults int) error {
	st := llrb.New()
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
	copy(s[common.AddressLength+common.IncarnationLength:], start)
	var lastKey common.Hash
	overrideCounter := 0
	min := &storageItem{key: common.BytesToHash(start)}
	if t, ok := r.storage[addr]; ok {
		t.AscendGreaterOrEqual(min, func(i llrb.Item) bool {
			item := i.(*storageItem)
			st.ReplaceOrInsert(item)
			if !item.value.IsZero() {
				copy(lastKey[:], item.key[:])
				// Only count non-zero items
				overrideCounter++
			}
			return overrideCounter < maxResults
		})
	}
	numDeletes := st.Len() - overrideCounter
	if err := state.WalkAsOf(r.tx, dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, s[:], 8*(common.AddressLength+common.IncarnationLength), r.blockNr+1, func(ks, vs []byte) (bool, error) {
		if !bytes.HasPrefix(ks, addr[:]) {
			return false, nil
		}
		if len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		key := ks[common.AddressLength:]
		//fmt.Printf("key: %x (%x)\n", key, ks)
		si := storageItem{}
		copy(si.key[:], key)
		if st.Has(&si) {
			return true, nil
		}
		si.value.SetBytes(vs)
		st.InsertNoReplace(&si)
		if bytes.Compare(key[:], lastKey[:]) > 0 {
			// Beyond overrides
			return st.Len() < maxResults+numDeletes, nil
		}
		return st.Len() < maxResults+overrideCounter+numDeletes, nil
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

// computeIntraBlockState retrieves the state database associated with a certain block.
// If no state is locally available for the given block, a number of blocks are
// attempted to be reexecuted to generate the desired state.
func ComputeIntraBlockState(tx ethdb.Tx, block *types.Block) (*state.IntraBlockState, *StateReader) {
	// If we have the state fully available, use that
	reader := NewStateReader(tx, block.NumberU64())
	statedb := state.New(reader)
	return statedb, reader
}
