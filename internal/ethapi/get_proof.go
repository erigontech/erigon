package ethapi

import (
	"bytes"
	"context"
	"math/big"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

// Result structs for GetProof
type AccountResult struct {
	Address      common.Address  `json:"address"`
	AccountProof []string        `json:"accountProof"`
	Balance      *hexutil.Big    `json:"balance"`
	CodeHash     common.Hash     `json:"codeHash"`
	Nonce        hexutil.Uint64  `json:"nonce"`
	StorageHash  common.Hash     `json:"storageHash"`
	StorageProof []StorageResult `json:"storageProof"`
}
type StorageResult struct {
	Key   string       `json:"key"`
	Value *hexutil.Big `json:"value"`
	Proof []string     `json:"proof"`
}

func (s *PublicBlockChainAPI) GetProof(ctx context.Context, address common.Address, storageKeys []string, blockNr rpc.BlockNumber) (*AccountResult, error) {
	block := uint64(blockNr.Int64()) + 1
	db := s.b.ChainDb()
	ts := dbutils.EncodeBlockNumber(block)
	accountMap := make(map[string]*accounts.Account)
	if err := changeset.Walk(db, dbutils.AccountChangeSetBucket, ts, 0, func(blockN uint64, k, v []byte) (bool, error) {
		k, v = common.CopyBytes(k), common.CopyBytes(v)
		if _, ok := accountMap[string(k)]; !ok {
			if len(v) > 0 {
				var a accounts.Account
				if innerErr := a.DecodeForStorage(v); innerErr != nil {
					return false, innerErr
				}
				accountMap[string(k)] = &a
			} else {
				accountMap[string(k)] = nil
			}
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	storageMap := make(map[string][]byte)
	if err := changeset.Walk(db, dbutils.AccountChangeSetBucket, ts, 0, func(blockN uint64, k, v []byte) (bool, error) {
		k, v = common.CopyBytes(k), common.CopyBytes(v)
		if _, ok := storageMap[string(k)]; !ok {
			storageMap[string(k)] = v
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	var unfurlList = make([]string, len(accountMap)+len(storageMap))
	unfurl := trie.NewRetainList(0)
	i := 0
	for ks, acc := range accountMap {
		unfurlList[i] = ks
		i++
		unfurl.AddKey([]byte(ks))
		if acc != nil {
			// Fill the code hashes
			if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
				if codeHash, err1 := db.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix([]byte(ks), acc.Incarnation)); err1 == nil {
					copy(acc.CodeHash[:], codeHash)
				} else {
					return nil, err1
				}
			}
		}
	}
	for ks := range storageMap {
		unfurlList[i] = ks
		i++
		var sk [64]byte
		copy(sk[:], []byte(ks)[:common.HashLength])
		copy(sk[common.HashLength:], []byte(ks)[common.HashLength+common.IncarnationLength:])
		unfurl.AddKey(sk[:])
	}
	rl := trie.NewRetainList(0)
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	rl.AddKey(addrHash[:])
	unfurl.AddKey(addrHash[:])
	for _, key := range storageKeys {
		keyAsHash := common.HexToHash(key)
		if keyHash, err1 := common.HashData(keyAsHash[:]); err1 == nil {
			trieKey := append(addrHash[:], keyHash[:]...)
			rl.AddKey(trieKey)
			unfurl.AddKey(trieKey)
		} else {
			return nil, err1
		}
	}
	sort.Strings(unfurlList)
	loader := trie.NewFlatDbSubTrieLoader()
	if err = loader.Reset(db, unfurl, unfurl, nil /* hashCollector */, [][]byte{nil}, []int{0}, false); err != nil {
		return nil, err
	}
	r := &Receiver{defaultReceiver: trie.NewDefaultReceiver(), unfurlList: unfurlList, accountMap: accountMap, storageMap: storageMap}
	r.defaultReceiver.Reset(rl, nil /* hashCollector */, false)
	loader.SetStreamReceiver(r)
	subTries, err1 := loader.LoadSubTries()
	if err1 != nil {
		return nil, err1
	}
	hash, err := rawdb.ReadCanonicalHash(db, block-1)
	if err != nil {
		return nil, err
	}
	header := rawdb.ReadHeader(db, hash, block-1)
	tr := trie.New(header.Root)
	if err = tr.HookSubTries(subTries, [][]byte{nil}); err != nil {
		return nil, err
	}
	accountProof, err2 := tr.Prove(addrHash[:], 0, false /* storage */)
	if err2 != nil {
		return nil, err2
	}
	storageProof := make([]StorageResult, len(storageKeys))
	for i, key := range storageKeys {
		keyAsHash := common.HexToHash(key)
		if keyHash, err1 := common.HashData(keyAsHash[:]); err1 == nil {
			trieKey := append(addrHash[:], keyHash[:]...)
			if proof, err3 := tr.Prove(trieKey, 64 /* nibbles to get to the storage sub-trie */, true /* storage */); err3 == nil {
				v, _ := tr.Get(trieKey)
				bv := new(big.Int)
				bv.SetBytes(v)
				storageProof[i] = StorageResult{key, (*hexutil.Big)(bv), toHexSlice(proof)}
			} else {
				return nil, err3
			}
		} else {
			return nil, err1
		}
	}
	acc, found := tr.GetAccount(addrHash[:])
	if !found {
		return nil, nil
	}
	return &AccountResult{
		Address:      address,
		AccountProof: toHexSlice(accountProof),
		Balance:      (*hexutil.Big)(acc.Balance.ToBig()),
		CodeHash:     acc.CodeHash,
		Nonce:        hexutil.Uint64(acc.Nonce),
		StorageHash:  acc.Root,
		StorageProof: storageProof,
	}, nil
}

type Receiver struct {
	defaultReceiver *trie.DefaultReceiver
	accountMap      map[string]*accounts.Account
	storageMap      map[string][]byte
	unfurlList      []string
	currentIdx      int
}

func (r *Receiver) Root() common.Hash { panic("don't call me") }
func (r *Receiver) Receive(
	itemType trie.StreamItem,
	accountKey []byte,
	storageKey []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	cutoff int,
) error {
	for r.currentIdx < len(r.unfurlList) {
		ks := r.unfurlList[r.currentIdx]
		k := []byte(ks)
		var c int
		switch itemType {
		case trie.StorageStreamItem, trie.SHashStreamItem:
			c = bytes.Compare(k, storageKey)
		case trie.AccountStreamItem, trie.AHashStreamItem:
			c = bytes.Compare(k, accountKey)
		case trie.CutoffStreamItem:
			c = -1
		}
		if c > 0 {
			return r.defaultReceiver.Receive(itemType, accountKey, storageKey, accountValue, storageValue, hash, cutoff)
		}
		if len(k) > common.HashLength {
			v := r.storageMap[ks]
			if c <= 0 && len(v) > 0 {
				if err := r.defaultReceiver.Receive(trie.StorageStreamItem, nil, k, nil, v, nil, 0); err != nil {
					return err
				}
			}
		} else {
			v := r.accountMap[ks]
			if c <= 0 && v != nil {
				if err := r.defaultReceiver.Receive(trie.AccountStreamItem, k, nil, v, nil, nil, 0); err != nil {
					return err
				}
			}
		}
		r.currentIdx++
		if c == 0 {
			return nil
		}
	}
	// We ran out of modifications, simply pass through
	return r.defaultReceiver.Receive(itemType, accountKey, storageKey, accountValue, storageValue, hash, cutoff)
}

func (r *Receiver) Result() trie.SubTries {
	return r.defaultReceiver.Result()
}
