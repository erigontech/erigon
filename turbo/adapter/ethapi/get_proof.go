package ethapi

import (
	"bytes"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

// Result structs for GetProof
type AccountResult struct {
	Address      libcommon.Address `json:"address"`
	AccountProof []string          `json:"accountProof"`
	Balance      *hexutil.Big      `json:"balance"`
	CodeHash     libcommon.Hash    `json:"codeHash"`
	Nonce        hexutil.Uint64    `json:"nonce"`
	StorageHash  libcommon.Hash    `json:"storageHash"`
	StorageProof []StorageResult   `json:"storageProof"`
}
type StorageResult struct {
	Key   string       `json:"key"`
	Value *hexutil.Big `json:"value"`
	Proof []string     `json:"proof"`
}

/*TODO: to support proofs
func (s *PublicBlockChainAPI) GetProof(ctx context.Context, address libcommon.Address, storageKeys []string, blockNr rpc.BlockNumber) (*AccountResult, error) {
	block := uint64(blockNr.Int64()) + 1
	db := s.b.ChainDb()
	ts := common2.EncodeTs(block)
	accountMap := make(map[string]*accounts.Account)
	if err := changeset.Walk(db, dbutils.AccountChangeSetBucket, ts, 0, func(blockN uint64, a, v []byte) (bool, error) {
		a, v = common.CopyBytes(a), common.CopyBytes(v)
		var kHash, err = common.HashData(a)
		if err != nil {
			return false, err
		}
		k := kHash[:]
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
	if err := changeset.Walk(db, dbutils.AccountChangeSetBucket, ts, 0, func(blockN uint64, a, v []byte) (bool, error) {
		a, v = common.CopyBytes(a), common.CopyBytes(v)
		var kHash, err = common.HashData(a)
		if err != nil {
			return true, err
		}
		k := kHash[:]
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
		copy(sk[:], []byte(ks)[:libcommon.HashLength])
		copy(sk[libcommon.HashLength:], []byte(ks)[libcommon.HashLength+common.IncarnationLength:])
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
		keyAsHash := libcommon.HexToHash(key)
		if keyHash, err1 := common.HashData(keyAsHash[:]); err1 == nil {
			trieKey := append(addrHash[:], keyHash[:]...)
			rl.AddKey(trieKey)
			unfurl.AddKey(trieKey)
		} else {
			return nil, err1
		}
	}
	sort.Strings(unfurlList)
	loader := trie.NewFlatDBTrieLoader("checkRoots")
	if err = loader.Reset(unfurl, nil, nil, false); err != nil {
		panic(err)
	}
	r := &Receiver{defaultReceiver: trie.NewDefaultReceiver(), unfurlList: unfurlList, accountMap: accountMap, storageMap: storageMap}
	r.defaultReceiver.Reset(rl, nil, false)
	loader.SetStreamReceiver(r)
	_, err = loader.CalcTrieRoot(db.(ethdb.HasTx).Tx().(ethdb.RwTx), []byte{}, nil)
	if err != nil {
		panic(err)
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
	accountProof, err2 := tr.Prove(addrHash[:], 0, false)
	if err2 != nil {
		return nil, err2
	}
	storageProof := make([]StorageResult, len(storageKeys))
	for i, key := range storageKeys {
		keyAsHash := libcommon.HexToHash(key)
		if keyHash, err1 := common.HashData(keyAsHash[:]); err1 == nil {
			trieKey := append(addrHash[:], keyHash[:]...)
			if proof, err3 := tr.Prove(trieKey, 64 , true); err3 == nil {
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
	return &AccountResult{}, nil
}
*/

type Receiver struct {
	defaultReceiver *trie.RootHashAggregator
	accountMap      map[string]*accounts.Account
	storageMap      map[string][]byte
	unfurlList      []string
	currentIdx      int
}

func (r *Receiver) Root() libcommon.Hash { panic("don't call me") }
func (r *Receiver) Receive(
	itemType trie.StreamItem,
	accountKey []byte,
	storageKey []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	hasTree bool,
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
			return r.defaultReceiver.Receive(itemType, accountKey, storageKey, accountValue, storageValue, hash, hasTree, cutoff)
		}
		if len(k) > length.Hash {
			v := r.storageMap[ks]
			if c <= 0 && len(v) > 0 {
				if err := r.defaultReceiver.Receive(trie.StorageStreamItem, nil, k, nil, v, nil, hasTree, 0); err != nil {
					return err
				}
			}
		} else {
			v := r.accountMap[ks]
			if c <= 0 && v != nil {
				if err := r.defaultReceiver.Receive(trie.AccountStreamItem, k, nil, v, nil, nil, hasTree, 0); err != nil {
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
	return r.defaultReceiver.Receive(itemType, accountKey, storageKey, accountValue, storageValue, hash, hasTree, cutoff)
}
