package commands

import (
	"bytes"

	"github.com/holiman/uint256"
	"github.com/petar/GoLLRB/llrb"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"golang.org/x/net/context"
)

type storageItem struct {
	key, seckey common.Hash
	value       uint256.Int
}

func (a *storageItem) Less(b llrb.Item) bool {
	bi := b.(*storageItem)
	return bytes.Compare(a.seckey[:], bi.seckey[:]) < 0
}

type StateReader struct {
	accountReads map[common.Address]struct{}
	storageReads map[common.Address]map[common.Hash]struct{}
	codeReads    map[common.Address]struct{}
	blockNr      uint64
	db           ethdb.KV
	storage      map[common.Address]*llrb.LLRB
}

func NewStateReader(db ethdb.KV, blockNr uint64) *StateReader {
	return &StateReader{
		accountReads: make(map[common.Address]struct{}),
		storageReads: make(map[common.Address]map[common.Hash]struct{}),
		codeReads:    make(map[common.Address]struct{}),
		db:           db,
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
	enc, err := state.GetAsOf(r.db, true /* plain */, false /* storage */, address[:], r.blockNr)
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
	enc, err := state.GetAsOf(r.db, true /* plain */, true /* storage */, compositeKey, r.blockNr)
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
	err := r.db.View(context.Background(), func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.CodeBucket)
		v, err := b.Get(codeHash[:])
		val = v
		return err
	})
	if err != nil {
		return nil, err
	}
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
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	_, err := h.Sha.Write(key[:])
	if err != nil {
		return err
	}
	i := &storageItem{key: *key, value: *value}
	_, err = h.Sha.Read(i.seckey[:])
	if err != nil {
		return err
	}

	t.ReplaceOrInsert(i)
	return nil
}

func (r *StateReader) CreateContract(address common.Address) error {
	delete(r.storage, address)
	return nil
}
