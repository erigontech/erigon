package smt

import (
	"math/big"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/TwiN/gocache/v2"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/log/v3"
)

type DB interface {
	Get(key utils.NodeKey) (utils.NodeValue12, error)
	Insert(key utils.NodeKey, value utils.NodeValue12) error
	GetAccountValue(key utils.NodeKey) (utils.NodeValue8, error)
	InsertAccountValue(key utils.NodeKey, value utils.NodeValue8) error
	InsertKeySource(key utils.NodeKey, value []byte) error
	GetKeySource(key utils.NodeKey) ([]byte, error)
	InsertHashKey(key utils.NodeKey, value utils.NodeKey) error
	GetHashKey(key utils.NodeKey) (utils.NodeKey, error)
	Delete(string) error

	SetLastRoot(lr *big.Int) error
	GetLastRoot() (*big.Int, error)

	OpenBatch(quitCh <-chan struct{})
	CommitBatch() error
	RollbackBatch()
}

type DebuggableDB interface {
	DB
	PrintDb()
	GetDb() map[string][]string
}

type SMT struct {
	Db                DB
	Cache             *gocache.Cache
	CacheHitFrequency map[string]int

	clearUpMutex sync.Mutex
}

type SMTResponse struct {
	NewRootScalar *utils.NodeKey
	Mode          string
}

func NewSMT(database DB) *SMT {
	if database == nil {
		database = db.NewMemDb()
	}

	return &SMT{
		Db:                database,
		Cache:             gocache.NewCache().WithMaxSize(10000).WithEvictionPolicy(gocache.LeastRecentlyUsed),
		CacheHitFrequency: make(map[string]int),
	}
}

func (s *SMT) LastRoot() *big.Int {
	s.clearUpMutex.Lock()
	defer s.clearUpMutex.Unlock()
	lr, err := s.Db.GetLastRoot()
	if err != nil {
		panic(err)
	}
	cop := new(big.Int).Set(lr)
	return cop
}

func (s *SMT) SetLastRoot(lr *big.Int) {
	s.clearUpMutex.Lock()
	defer s.clearUpMutex.Unlock()
	err := s.Db.SetLastRoot(lr)
	if err != nil {
		panic(err)
	}
}

func (s *SMT) StartPeriodicCheck(doneChan chan bool) {
	if _, ok := s.Db.(*db.EriDb); ok {
		log.Warn("mdbx tx cannot be used in goroutine - periodic check disabled")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-doneChan:
				cancel()
				return
			case <-ticker.C:
				// start timer
				start := time.Now()
				ct := s.CheckOrphanedNodes(ctx)
				elapsed := time.Since(start)
				fmt.Printf("CheckOrphanedNodes took %s removing %d orphaned nodes\n", elapsed, ct)
			}
		}
	}()
}

func (s *SMT) InsertBI(key *big.Int, value *big.Int) (*SMTResponse, error) {
	k := utils.ScalarToNodeKey(key)
	v := utils.ScalarToNodeValue8(value)
	return s.insertSingle(k, v, [4]uint64{})
}

func (s *SMT) InsertKA(key utils.NodeKey, value *big.Int) (*SMTResponse, error) {
	x := utils.ScalarToArrayBig(value)
	v, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, err
	}

	return s.insertSingle(key, *v, [4]uint64{})
}

func (s *SMT) InsertStorage(ethAddr string, storage *map[string]string, chm *map[string]*utils.NodeValue8, vhm *map[string][4]uint64) (*SMTResponse, error) {
	s.clearUpMutex.Lock()
	defer s.clearUpMutex.Unlock()

	a := utils.ConvertHexToBigInt(ethAddr)
	add := utils.ScalarToArrayBig(a)

	or, err := s.getLastRoot()
	if err != nil {
		return nil, err
	}

	smtr := &SMTResponse{
		NewRootScalar: &or,
	}
	for k := range *storage {
		keyStoragePosition, err := utils.KeyContractStorage(add, k)
		if err != nil {
			return nil, err
		}

		smtr, err = s.insert(keyStoragePosition, *(*chm)[k], (*vhm)[k], *smtr.NewRootScalar)
		if err != nil {
			return nil, err
		}

		sp, _ := utils.StrValToBigInt(k)

		ks := utils.EncodeKeySource(utils.SC_STORAGE, utils.ConvertHexToAddress(ethAddr), common.BigToHash(sp))
		err = s.Db.InsertKeySource(keyStoragePosition, ks)

		if err != nil {
			return nil, err
		}
	}

	if err = s.setLastRoot(*smtr.NewRootScalar); err != nil {
		return nil, err
	}

	return smtr, nil
}

func (s *SMT) insertSingle(k utils.NodeKey, v utils.NodeValue8, newValH [4]uint64) (*SMTResponse, error) {
	s.clearUpMutex.Lock()
	defer s.clearUpMutex.Unlock()

	or, err := s.getLastRoot()
	if err != nil {
		return nil, err
	}

	smtr, err := s.insert(k, v, newValH, or)
	if err != nil {
		return nil, err
	}

	if err = s.setLastRoot(*smtr.NewRootScalar); err != nil {
		return nil, err
	}

	return smtr, nil
}

func (s *SMT) insert(k utils.NodeKey, v utils.NodeValue8, newValH [4]uint64, oldRoot utils.NodeKey) (*SMTResponse, error) {
	newRoot := oldRoot

	smtResponse := &SMTResponse{
		Mode: "not run",
	}

	// split the key
	keys := k.GetPath()

	var usedKey []int
	var level int
	var foundKey *utils.NodeKey
	var foundVal utils.NodeValue8
	var foundRKey utils.NodeKey
	var proofHashCounter int
	var foundOldValHash utils.NodeKey

	siblings := map[int]*utils.NodeValue12{}

	var err error
	// JS WHILE
	for !oldRoot.IsZero() && foundKey == nil {
		sl, err := s.Db.Get(oldRoot)
		if err != nil {
			return nil, err
		}
		siblings[level] = &sl
		if siblings[level].IsFinalNode() {
			foundOldValHash = utils.NodeKeyFromBigIntArray(siblings[level][4:8])
			fva, err := s.Db.Get(foundOldValHash)
			if err != nil {
				return nil, err
			}
			foundValA := utils.Value8FromBigIntArray(fva[0:8])
			foundRKey = utils.NodeKeyFromBigIntArray(siblings[level][0:4])
			foundVal = foundValA

			foundKey = utils.JoinKey(usedKey, foundRKey)
			if err != nil {
				return nil, err
			}
		} else {
			oldRoot = utils.NodeKeyFromBigIntArray(siblings[level][keys[level]*4 : keys[level]*4+4])
			usedKey = append(usedKey, keys[level])
			level++
		}
	}

	level--
	if len(usedKey) != 0 {
		usedKey = usedKey[:len(usedKey)-1]
	}

	proofHashCounter = 0
	if !oldRoot.IsZero() {
		//utils.RemoveOver(siblings, level+1)
		proofHashCounter += len(siblings)
		if foundVal.IsZero() {
			proofHashCounter += 2
		}
	}

	if !v.IsZero() { // we have a value - so we're updating or inserting
		if foundKey != nil {
			if foundKey.IsEqualTo(k) {
				// UPDATE MODE
				smtResponse.Mode = "update"

				if newValH == [4]uint64{} {
					newValH, err = s.hashcalcAndSave(v.ToUintArray(), utils.BranchCapacity)
				} else {
					newValH, err = s.hashSave(v.ToUintArray(), utils.BranchCapacity, newValH)
				}
				if err != nil {
					return nil, err
				}

				newLeafHash, err := s.hashcalcAndSave(utils.ConcatArrays4(foundRKey, newValH), utils.LeafCapacity)
				if err != nil {
					return nil, err
				}
				s.Db.InsertHashKey(newLeafHash, k)
				if level >= 0 {
					for j := 0; j < 4; j++ {
						siblings[level][keys[level]*4+j] = new(big.Int).SetUint64(newLeafHash[j])
					}
				} else {
					newRoot = newLeafHash
				}
			} else {
				smtResponse.Mode = "insertFound"
				// INSERT WITH FOUND KEY
				level2 := level + 1
				foundKeys := foundKey.GetPath()

				for {
					if level2 >= len(keys) || level2 >= len(foundKeys) {
						break
					}

					if keys[level2] != foundKeys[level2] {
						break
					}

					level2++
				}

				oldKey := utils.RemoveKeyBits(*foundKey, level2+1)
				oldLeafHash, err := s.hashcalcAndSave(utils.ConcatArrays4(oldKey, foundOldValHash), utils.LeafCapacity)
				s.Db.InsertHashKey(oldLeafHash, *foundKey)
				if err != nil {
					return nil, err
				}

				newKey := utils.RemoveKeyBits(k, level2+1)

				if newValH == [4]uint64{} {
					newValH, err = s.hashcalcAndSave(v.ToUintArray(), utils.BranchCapacity)
				} else {
					newValH, err = s.hashSave(v.ToUintArray(), utils.BranchCapacity, newValH)
				}

				if err != nil {
					return nil, err
				}

				newLeafHash, err := s.hashcalcAndSave(utils.ConcatArrays4(newKey, newValH), utils.LeafCapacity)
				if err != nil {
					return nil, err
				}

				s.Db.InsertHashKey(newLeafHash, k)

				var node [8]uint64
				for i := 0; i < 8; i++ {
					node[i] = 0
				}

				for j := 0; j < 4; j++ {
					node[keys[level2]*4+j] = newLeafHash[j]
					node[foundKeys[level2]*4+j] = oldLeafHash[j]
				}

				r2, err := s.hashcalcAndSave(node, utils.BranchCapacity)
				if err != nil {
					return nil, err
				}
				proofHashCounter += 4
				level2 -= 1

				for level2 != level {
					for i := 0; i < 8; i++ {
						node[i] = 0
					}

					for j := 0; j < 4; j++ {
						node[keys[level2]*4+j] = r2[j]
					}

					r2, err = s.hashcalcAndSave(node, utils.BranchCapacity)
					if err != nil {
						return nil, err
					}
					proofHashCounter += 1
					level2 -= 1
				}

				if level >= 0 {
					for j := 0; j < 4; j++ {
						siblings[level][keys[level]*4+j] = new(big.Int).SetUint64(r2[j])
					}
				} else {
					newRoot = r2
				}
			}

		} else {
			// INSERT NOT FOUND
			smtResponse.Mode = "insertNotFound"
			newKey := utils.RemoveKeyBits(k, level+1)

			if newValH == [4]uint64{} {
				newValH, err = s.hashcalcAndSave(v.ToUintArray(), utils.BranchCapacity)
			} else {
				newValH, err = s.hashSave(v.ToUintArray(), utils.BranchCapacity, newValH)
			}
			if err != nil {
				return nil, err
			}

			nk := utils.ConcatArrays4(newKey, newValH)

			newLeafHash, err := s.hashcalcAndSave(nk, utils.LeafCapacity)
			if err != nil {
				return nil, err
			}

			s.Db.InsertHashKey(newLeafHash, k)

			proofHashCounter += 2

			if level >= 0 {
				for j := 0; j < 4; j++ {
					nlh := big.Int{}
					nlh.SetUint64(newLeafHash[j])
					siblings[level][keys[level]*4+j] = &nlh
				}
			} else {
				newRoot = newLeafHash
			}
		}
	} else if foundKey != nil && foundKey.IsEqualTo(k) { // we don't have a value so we're deleting
		if level >= 0 {
			for j := 0; j < 4; j++ {
				siblings[level][keys[level]*4+j] = big.NewInt(0)
			}

			uKey, err := siblings[level].IsUniqueSibling()
			if err != nil {
				return nil, err
			}

			if uKey >= 0 {
				// DELETE FOUND
				smtResponse.Mode = "deleteFound"
				dk := utils.NodeKeyFromBigIntArray(siblings[level][uKey*4 : uKey*4+4])
				sl, err := s.Db.Get(dk)
				if err != nil {
					return nil, err
				}
				siblings[level+1] = &sl

				if siblings[level+1].IsFinalNode() {
					valH := siblings[level+1].Get4to8()

					rKey := siblings[level+1].Get0to4()
					proofHashCounter += 2

					insKey := utils.JoinKey(append(usedKey, uKey), *rKey)

					for uKey >= 0 && level >= 0 {
						level -= 1
						if level >= 0 {
							uKey, err = siblings[level].IsUniqueSibling()
							if err != nil {
								return nil, err
							}
						}
					}

					oldKey := utils.RemoveKeyBits(*insKey, level+1)
					oldLeafHash, err := s.hashcalcAndSave(utils.ConcatArrays4(oldKey, *valH), utils.LeafCapacity)
					s.Db.InsertHashKey(oldLeafHash, *insKey)
					if err != nil {
						return nil, err
					}
					proofHashCounter += 1

					if level >= 0 {
						for j := 0; j < 4; j++ {
							siblings[level][keys[level]*4+j] = new(big.Int).SetUint64(oldLeafHash[j])
						}
					} else {
						newRoot = oldLeafHash
					}
				} else {
					// DELETE NOT FOUND
					smtResponse.Mode = "deleteNotFound"
				}
			} else {
				// DELETE NOT FOUND
				smtResponse.Mode = "deleteNotFound"
			}
		} else {
			// DELETE LAST
			smtResponse.Mode = "deleteLast"
			newRoot = utils.NodeKey{0, 0, 0, 0}
		}
	} else { // we're going zero to zero - do nothing
		smtResponse.Mode = "zeroToZero"
	}

	utils.RemoveOver(siblings, level+1)

	for level >= 0 {
		hashValueIn, err := utils.NodeValue8FromBigIntArray(siblings[level][0:8])
		if err != nil {
			return nil, err
		}
		hashCapIn := utils.NodeKeyFromBigIntArray(siblings[level][8:12])
		newRoot, err = s.hashcalcAndSave(hashValueIn.ToUintArray(), hashCapIn)
		if err != nil {
			return nil, err
		}
		proofHashCounter += 1
		level -= 1
		if level >= 0 {
			for j := 0; j < 4; j++ {
				nrj := big.Int{}
				nrj.SetUint64(newRoot[j])
				siblings[level][keys[level]*4+j] = &nrj
			}
		}
	}

	_ = oldRoot

	smtResponse.NewRootScalar = &newRoot

	return smtResponse, nil
}

func (s *SMT) hashSave(in [8]uint64, capacity, h [4]uint64) ([4]uint64, error) {
	cacheKey := fmt.Sprintf("%v-%v", in, capacity)
	if cachedValue, exists := s.Cache.Get(cacheKey); exists {
		s.CacheHitFrequency[cacheKey]++
		return cachedValue.([4]uint64), nil
	}

	var sl []uint64
	sl = append(sl, in[:]...)
	sl = append(sl, capacity[:]...)

	v := utils.NodeValue12{}
	for i, val := range sl {
		b := new(big.Int)
		v[i] = b.SetUint64(val)
	}

	err := s.Db.Insert(h, v)

	s.Cache.Set(cacheKey, h)

	return h, err
}

func (s *SMT) hashcalcAndSave(in [8]uint64, capacity [4]uint64) ([4]uint64, error) {
	h, err := utils.Hash(in, capacity)
	if err != nil {
		return [4]uint64{}, err
	}

	return s.hashSave(in, capacity, h)
}

func (s *SMT) getLastRoot() (utils.NodeKey, error) {
	or, err := s.Db.GetLastRoot()
	if err != nil {
		return utils.NodeKey{}, err
	}
	return utils.ScalarToRoot(or), nil
}

func (s *SMT) setLastRoot(newRoot utils.NodeKey) error {
	return s.Db.SetLastRoot(newRoot.ToBigInt())
}

// Utility functions for debugging

func (s *SMT) PrintDb() {
	if debugDB, ok := s.Db.(DebuggableDB); ok {
		debugDB.PrintDb()
	}
}

func (s *SMT) PrintTree() {
	if debugDB, ok := s.Db.(DebuggableDB); ok {
		data := debugDB.GetDb()
		str, err := json.Marshal(data)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(str))
	}
}

func (s *SMT) PrintCacheHitsByFrequency() {
	type kv struct {
		Key   string
		Value int
	}

	fmt.Println("SMT Cache Hits:")

	var ss []kv
	for k, v := range s.CacheHitFrequency {
		if v > 1 {
			ss = append(ss, kv{k, v})
		}
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	for i, kv := range ss {
		if i > 10 {
			break
		}
		fmt.Printf("%s: %d\n", kv.Key, kv.Value)
	}
}

type VisitedNodesMap map[string]bool

func (s *SMT) CheckOrphanedNodes(ctx context.Context) int {
	if _, ok := s.Db.(*db.EriDb); ok {
		log.Warn("mdbx tx cannot be used in goroutine - periodic check disabled")
		return 0
	}

	s.clearUpMutex.Lock()
	defer s.clearUpMutex.Unlock()

	visited := make(VisitedNodesMap)

	root, err := s.Db.GetLastRoot()
	if err != nil {
		return 0
	}

	err = s.traverseAndMark(ctx, root, visited)
	if err != nil {
		return 0
	}

	debugDB, ok := s.Db.(DebuggableDB)
	if !ok {
		log.Warn("db is not cleanable")
	}

	orphanedNodes := make([]string, 0)
	for dbKey := range debugDB.GetDb() {
		if _, ok := visited[dbKey]; !ok {
			orphanedNodes = append(orphanedNodes, dbKey)
		}
	}

	rootKey := utils.ConvertBigIntToHex(root)

	for _, node := range orphanedNodes {
		if node == rootKey {
			continue
		}
		err := s.Db.Delete(node)
		if err != nil {
			log.Warn("failed to delete orphaned node", "node", node, "err", err)
		}
	}

	return len(orphanedNodes)
}

type TraverseAction func(prefix []byte, k utils.NodeKey, v utils.NodeValue12) (bool, error)

func (s *SMT) Traverse(ctx context.Context, node *big.Int, action TraverseAction) error {
	if node == nil || node.Cmp(big.NewInt(0)) == 0 {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ky := utils.ScalarToRoot(node)

	nodeValue, err := s.Db.Get(ky)

	if err != nil {
		return err
	}

	shouldContinue, err := action(nil, ky, nodeValue)

	if err != nil {
		return err
	}

	if nodeValue.IsFinalNode() || !shouldContinue {
		return nil
	}

	for i := 0; i < 2; i++ {
		if len(nodeValue) < i*4+4 {
			return errors.New("nodeValue has insufficient length")
		}
		child := utils.NodeKeyFromBigIntArray(nodeValue[i*4 : i*4+4])
		err := s.Traverse(ctx, child.ToBigInt(), action)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	return nil
}

func (s *SMT) traverseAndMark(ctx context.Context, node *big.Int, visited VisitedNodesMap) error {
	return s.Traverse(ctx, node, func(prefix []byte, k utils.NodeKey, v utils.NodeValue12) (bool, error) {
		if visited[utils.ConvertBigIntToHex(k.ToBigInt())] {
			return false, nil
		}

		visited[utils.ConvertBigIntToHex(k.ToBigInt())] = true
		return true, nil
	})
}
