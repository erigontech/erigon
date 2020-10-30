package shards

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

//go:generate protoc --proto_path=. --go_out=.. --go-grpc_out=.. "shards.proto" -I=. -I=./../../build/include/google

var emptyCodeHash = crypto.Keccak256(nil)

// Implements StateReader and StateWriter, for specified shard
type Shard struct {
	tx            ethdb.Tx
	blockNr       uint64
	accountCache  *fastcache.Cache
	storageCache  *fastcache.Cache
	codeCache     *fastcache.Cache
	codeSizeCache *fastcache.Cache
	client        Dispatcher_StartDispatchClient
	shardBits     int  // Number of high bits in the key that determine the shard ID (maximum 7)
	shardID       byte // Shard ID in the lower bit
}

func NewShard(tx ethdb.Tx,
	blockNr uint64,
	client Dispatcher_StartDispatchClient,
	accountCache, storageCache, codeCache, codeSizeCache *fastcache.Cache,
	shardBits int,
	shardID byte,
) *Shard {
	shard := &Shard{
		tx:        tx,
		blockNr:   blockNr,
		client:    client,
		shardBits: shardBits,
		shardID:   shardID,
	}
	shard.SetAccountCache(accountCache)
	shard.SetStorageCache(storageCache)
	shard.SetCodeCache(codeCache)
	shard.SetCodeSizeCache(codeSizeCache)
	return shard
}

func (s *Shard) SetAccountCache(accountCache *fastcache.Cache) {
	s.accountCache = accountCache
}

func (s *Shard) SetStorageCache(storageCache *fastcache.Cache) {
	s.storageCache = storageCache
}

func (s *Shard) SetCodeCache(codeCache *fastcache.Cache) {
	s.codeCache = codeCache
}

func (s *Shard) SetCodeSizeCache(codeSizeCache *fastcache.Cache) {
	s.codeSizeCache = codeSizeCache
}

func (s *Shard) SetBlockNr(blockNr uint64) {
	s.blockNr = blockNr
}

func (s *Shard) GetBlockNr() uint64 {
	return s.blockNr
}

func (s *Shard) isMyShard(firstByte byte) bool {
	return (firstByte >> (8 - s.shardBits)) == s.shardID
}

func (s *Shard) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var enc []byte
	var ok bool
	if !s.isMyShard(address[0]) {
		stateRead, err := s.client.Recv()
		if err != nil {
			return nil, fmt.Errorf("reading remote state for ReadAccountData %x: %w", address, err)
		}
		if !bytes.Equal(stateRead.K, address.Bytes()) {
			return nil, fmt.Errorf("read mismatched key, expected %x, got %x", address, stateRead.K)
		}
		enc = stateRead.V
	} else {
		if s.accountCache != nil {
			enc, ok = s.accountCache.HasGet(nil, address[:])
		}
		if !ok {
			var err error
			enc, err = state.GetAsOf(s.tx, false /* storage */, address[:], s.blockNr+1)
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return nil, err
			}
		}
		if !ok && s.accountCache != nil {
			s.accountCache.Set(address[:], enc)
		}
		if err := s.client.Send(&StateRead{K: address.Bytes(), V: enc}); err != nil {
			return nil, fmt.Errorf("sending remove state for ReadAccountData %x: %w", address, err)
		}
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	//restore codehash
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		codeHash, err := s.tx.GetOne(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation))
		if err != nil {
			return nil, err
		}
		if len(codeHash) > 0 {
			acc.CodeHash = common.BytesToHash(codeHash)
		}
	}
	return &acc, nil
}

func (s *Shard) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key)
	var enc []byte
	var ok bool
	if !s.isMyShard(address[0]) {
		stateRead, err := s.client.Recv()
		if err != nil {
			return nil, fmt.Errorf("reading remote state for ReadAccountStorage %d: %w", compositeKey, err)
		}
		if !bytes.Equal(stateRead.K, compositeKey) {
			return nil, fmt.Errorf("read mismatched key, expected %x, got %x", compositeKey, stateRead.K)
		}
		enc = stateRead.V
	} else {
		if s.storageCache != nil {
			enc, ok = s.storageCache.HasGet(nil, compositeKey)
		}
		if !ok {
			var err error
			enc, err = state.GetAsOf(s.tx, true /* storage */, compositeKey, s.blockNr+1)
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return nil, err
			}
		}
		if !ok && s.storageCache != nil {
			s.storageCache.Set(compositeKey, enc)
		}
		if err := s.client.Send(&StateRead{K: compositeKey, V: enc}); err != nil {
			return nil, fmt.Errorf("sending remove state for ReadAccountStorage %x: %w", compositeKey, err)
		}
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (s *Shard) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	var code []byte
	var ok bool
	if !s.isMyShard(address[0]) {
		stateRead, err := s.client.Recv()
		if err != nil {
			return nil, fmt.Errorf("reading remote state for ReadAccountCode %x: %w", address, err)
		}
		if !bytes.Equal(stateRead.K, address.Bytes()) {
			return nil, fmt.Errorf("read mismatched key, expected %x, got %x", address, stateRead.K)
		}
		code = stateRead.V
	} else {
		if s.codeCache != nil {
			code, ok = s.codeCache.HasGet(nil, address[:])
		}
		if !ok {
			var err error
			code, err = ethdb.Get(s.tx, dbutils.CodeBucket, codeHash[:])
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return nil, err
			}
		}
		if !ok && s.codeCache != nil && len(code) <= 1024 {
			s.codeCache.Set(address[:], code)
		}
		if !ok && s.codeSizeCache != nil {
			var b [4]byte
			binary.BigEndian.PutUint32(b[:], uint32(len(code)))
			s.codeSizeCache.Set(address[:], b[:])
		}
		if err := s.client.Send(&StateRead{K: address.Bytes(), V: code}); err != nil {
			return nil, fmt.Errorf("sending remove state for ReadAccountCode %x: %w", address, err)
		}
	}
	return code, nil
}

func (s *Shard) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}
	var size int
	var ok bool
	if !s.isMyShard(address[0]) {
		stateRead, err := s.client.Recv()
		if err != nil {
			return 0, fmt.Errorf("reading remote state for ReadAccountCodeSize %x: %w", address, err)
		}
		if !bytes.Equal(stateRead.K, address.Bytes()) {
			return 0, fmt.Errorf("read mismatched key, expected %x, got %x", address, stateRead.K)
		}
		size = int(binary.BigEndian.Uint32(stateRead.V))
	} else {
		if s.codeSizeCache != nil {
			var b []byte
			if b, ok = s.codeSizeCache.HasGet(nil, address[:]); ok {
				size = int(binary.BigEndian.Uint32(b))
			}
		}
		if !ok {
			code, err := ethdb.Get(s.tx, dbutils.CodeBucket, codeHash[:])
			if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
				return 0, err
			}
			size = len(code)
		}
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(size))
		if !ok && s.codeSizeCache != nil {
			s.codeSizeCache.Set(address[:], b[:])
		}
		if err := s.client.Send(&StateRead{K: address.Bytes(), V: b[:]}); err != nil {
			return 0, fmt.Errorf("sending remove state for ReadAccountCodeSize %x: %w", address, err)
		}
	}
	return size, nil
}

func (s *Shard) ReadAccountIncarnation(address common.Address) (uint64, error) {
	enc, err := state.GetAsOf(s.tx, false /* storage */, address[:], s.blockNr+2)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, err
	}
	if len(enc) == 0 {
		return 0, nil
	}
	var acc accounts.Account
	if err = acc.DecodeForStorage(enc); err != nil {
		return 0, err
	}
	if acc.Incarnation == 0 {
		return 0, nil
	}
	return acc.Incarnation - 1, nil
}

func (s *Shard) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if !s.isMyShard(address[0]) {
		return nil
	}
	if s.accountCache != nil {
		value := make([]byte, account.EncodingLengthForStorage())
		account.EncodeForStorage(value)
		s.accountCache.Set(address[:], value)
	}
	return nil
}

func (s *Shard) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if !s.isMyShard(address[0]) {
		return nil
	}
	if s.codeCache != nil {
		if len(code) <= 1024 {
			s.codeCache.Set(address[:], code)
		} else {
			s.codeCache.Del(address[:])
		}
	}
	if s.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		s.codeSizeCache.Set(address[:], b[:])
	}
	return nil
}

func (s *Shard) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if !s.isMyShard(address[0]) {
		return nil
	}
	if s.accountCache != nil {
		s.accountCache.Set(address[:], nil)
	}
	if s.codeCache != nil {
		s.codeCache.Set(address[:], nil)
	}
	if s.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], 0)
		s.codeSizeCache.Set(address[:], b[:])
	}
	return nil
}

func (s *Shard) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	if !s.isMyShard(address[0]) {
		return nil
	}
	if s.storageCache != nil {
		compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key)
		s.storageCache.Set(compositeKey, value.Bytes())
	}
	return nil
}

func (s *Shard) CreateContract(address common.Address) error {
	return nil
}

func (s *Shard) WriteChangeSets() error {
	return nil
}

func (s *Shard) WriteHistory() error {
	return nil
}

func (s *Shard) ChangeSetWriter() *state.ChangeSetWriter {
	return nil
}
