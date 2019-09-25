// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/ledgerwatch/turbo-geth/log"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/trie"
)

/* Proof Of Concept for verification of Stateless client proofs */
type CodeTime struct {
	bytecode []byte
	t        uint64
}

type Stateless struct {
	blockNr        uint64
	t              *trie.Trie
	storageTries   map[common.Address]*trie.Trie
	codeMap        map[common.Hash]CodeTime
	timeToCodeHash map[uint64]map[common.Hash]struct{}
	trace          bool
	storageUpdates map[common.Address]map[common.Hash][]byte
	accountUpdates map[common.Hash]*accounts.Account
	deleted        map[common.Hash]struct{}
	tp             *trie.TriePruning
}

func NewStateless(stateRoot common.Hash,
	blockProof trie.BlockProof,
	blockNr uint64,
	trace bool,
) (*Stateless, error) {
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	tp := trie.NewTriePruning(blockNr)

	if trace {
		fmt.Printf("ACCOUNT TRIE ==============================================\n")
	}
	touchFunc := func(hex []byte, del bool) {
		tp.Touch(hex, del)
	}
	t, _, _, _, _ := trie.NewFromProofs(touchFunc, blockNr, false, blockProof.Masks, blockProof.ShortKeys, blockProof.Values, blockProof.Hashes, trace)
	t.SetTouchFunc(touchFunc)
	if stateRoot != t.Hash() {
		filename := fmt.Sprintf("root_%d.txt", blockNr)
		f, err := os.Create(filename)
		if err == nil {
			defer f.Close()
			t.Print(f)
		}
		return nil, fmt.Errorf("Expected root: %x, Constructed root: %x", stateRoot, t.Hash())
	}
	storageTries := make(map[common.Address]*trie.Trie)
	var maskIdx, hashIdx, shortIdx, valueIdx int
	for _, contract := range blockProof.Contracts {
		if trace {
			fmt.Printf("TRIE %x ==============================================\n", contract)
		}
		//contractCopy := contract
		touchFunc := func(hex []byte, del bool) {
			//tp.TouchStorage(contractCopy, hex, del)
		}
		st, mIdx, hIdx, sIdx, vIdx := trie.NewFromProofs(touchFunc, blockNr, true,
			blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:], trace)
		st.SetTouchFunc(touchFunc)
		h.Sha.Reset()
		if _, err := h.Sha.Write(contract[:]); err != nil {
			return nil, err
		}
		var addrHash common.Hash
		if _, err := h.Sha.Read(addrHash[:]); err != nil {
			return nil, err
		}
		storageTries[contract] = st
		enc, ok := t.Get(addrHash[:])
		if !ok {
			return nil, fmt.Errorf("[THIN] account %x (hash %x) is not present in the proof", contract, addrHash)
		}
		var acc accounts.Account
		if err := acc.DecodeForHashing(enc); err != nil {
			return nil, err
		}
		if acc.Root != st.Hash() {
			filename := fmt.Sprintf("root_%d.txt", blockNr-1)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close() //nolint
				st.Print(f)
			}
			return nil, fmt.Errorf("[THIN] Expected storage root for %x: %x, constructed root: %x", contract, acc.Root, st.Hash())
		}
		maskIdx += mIdx
		shortIdx += sIdx
		hashIdx += hIdx
		valueIdx += vIdx
	}
	codeMap := make(map[common.Hash]CodeTime)
	timeToCodeHash := make(map[uint64]map[common.Hash]struct{})
	var codeHash common.Hash
	for _, code := range blockProof.Codes {
		h.Sha.Reset()
		if _, err := h.Sha.Write(code); err != nil {
			return nil, err
		}
		if _, err := h.Sha.Read(codeHash[:]); err != nil {
			return nil, err
		}
		if codeTime, ok := codeMap[codeHash]; ok {
			m := timeToCodeHash[codeTime.t]
			delete(m, codeHash)
			if len(m) == 0 {
				delete(timeToCodeHash, codeTime.t)
			}
		}
		codeMap[codeHash] = CodeTime{bytecode: code, t: blockNr}
		if m, ok := timeToCodeHash[blockNr]; ok {
			m[codeHash] = struct{}{}
		} else {
			m = make(map[common.Hash]struct{})
			timeToCodeHash[blockNr] = m
			m[codeHash] = struct{}{}
		}
	}
	return &Stateless{
		blockNr:        blockNr,
		t:              t,
		storageTries:   storageTries,
		codeMap:        codeMap,
		timeToCodeHash: timeToCodeHash,
		trace:          trace,
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*accounts.Account),
		deleted:        make(map[common.Hash]struct{}),
		tp:             tp,
	}, nil
}

func (s *Stateless) ThinProof(blockProof trie.BlockProof, blockNr uint64, cuttime uint64, trace bool) trie.BlockProof {
	if blockNr != s.tp.BlockNr() {
		panic(fmt.Sprintf("blockNr %d != s.tp.BlockNr() %d", blockNr, s.tp.BlockNr()))
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	if trace {
		fmt.Printf("THIN\n")
	}
	var aMasks, acMasks []uint16
	var aShortKeys, acShortKeys [][]byte
	var aValues, acValues [][]byte
	var aHashes, acHashes []common.Hash
	timeFunc := func(hex []byte) uint64 {
		return s.tp.Timestamp(hex)
	}
	_, _, _, _, aMasks, aShortKeys, aValues, aHashes = s.t.AmmendProofs(timeFunc, cuttime, blockProof.Masks, blockProof.ShortKeys, blockProof.Values, blockProof.Hashes,
		aMasks, aShortKeys, aValues, aHashes, trace)
	var maskIdx, hashIdx, shortIdx, valueIdx int
	aContracts := []common.Address{}
	for _, contract := range blockProof.Contracts {
		if trace {
			fmt.Printf("THIN TRIE %x ==============================================\n", contract)
		}
		h.Sha.Reset()
		if _, err := h.Sha.Write(contract[:]); err != nil {
			log.Error("error on sha", "err", err)
		}
		var addrHash common.Hash
		if _, err := h.Sha.Read(addrHash[:]); err != nil {
			log.Error("error on sha", "err", err)
		}
		var st *trie.Trie
		var ok bool
		var mIdx, hIdx, sIdx, vIdx int
		if st, ok = s.storageTries[contract]; !ok {
			touchFunc := func(hex []byte, del bool) {}
			_, mIdx, hIdx, sIdx, vIdx = trie.NewFromProofs(touchFunc, blockNr, true,
				blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:], trace)
			if mIdx > 0 {
				acMasks = append(acMasks, blockProof.CMasks[maskIdx:maskIdx+mIdx]...)
			}
			if sIdx > 0 {
				acShortKeys = append(acShortKeys, blockProof.CShortKeys[shortIdx:shortIdx+sIdx]...)
			}
			if vIdx > 0 {
				acValues = append(acValues, blockProof.CValues[valueIdx:valueIdx+vIdx]...)
			}
			if hIdx > 0 {
				acHashes = append(acHashes, blockProof.CHashes[hashIdx:hashIdx+hIdx]...)
			}
			aContracts = append(aContracts, contract)
		} else {
			//contractCopy := contract
			timeFunc := func(hex []byte) uint64 {
				return 0
				//return s.tp.TimestampContract(contractCopy, hex)
			}
			mIdx, hIdx, sIdx, vIdx, acMasks, acShortKeys, acValues, acHashes = st.AmmendProofs(timeFunc, cuttime,
				blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:],
				acMasks, acShortKeys, acValues, acHashes,
				trace)
			aContracts = append(aContracts, contract)
		}
		maskIdx += mIdx
		shortIdx += sIdx
		hashIdx += hIdx
		valueIdx += vIdx
	}
	aCodes := [][]byte{}
	var codeHash common.Hash
	for _, code := range blockProof.Codes {
		h.Sha.Reset()
		if _, err := h.Sha.Write(code); err != nil {
			log.Error("error on sha", "err", err)
		}
		if _, err := h.Sha.Read(codeHash[:]); err != nil {
			log.Error("error on sha", "err", err)
		}
		if codeTime, ok := s.codeMap[codeHash]; ok {
			if trace {
				fmt.Printf("%d Code hash: %x, codeTime.t: %d, cuttime: %d\n", blockNr, codeHash, codeTime.t, cuttime)
			}
			if codeTime.t < cuttime {
				aCodes = append(aCodes, code)
			}
		} else {
			if trace {
				fmt.Printf("%d Code hash: %x not found in codeMap, adding\n", blockNr, codeHash)
			}
			aCodes = append(aCodes, code)
		}
	}
	return trie.BlockProof{aContracts, acMasks, acHashes, acShortKeys, acValues, aCodes, aMasks, aHashes, aShortKeys, aValues}
}

func (s *Stateless) touchCodeHash(codeHash common.Hash, code []byte, blockNr uint64) {
	if codeTime, ok := s.codeMap[codeHash]; ok {
		m := s.timeToCodeHash[codeTime.t]
		delete(m, codeHash)
		if len(m) == 0 {
			delete(s.timeToCodeHash, codeTime.t)
		}
	}
	s.codeMap[codeHash] = CodeTime{bytecode: code, t: blockNr}
	if m, ok := s.timeToCodeHash[blockNr]; ok {
		m[codeHash] = struct{}{}
	} else {
		m = make(map[common.Hash]struct{})
		s.timeToCodeHash[blockNr] = m
		m[codeHash] = struct{}{}
	}
}

func (s *Stateless) ApplyProof(stateRoot common.Hash, blockProof trie.BlockProof,
	blockNr uint64,
	trace bool,
) error {
	if blockNr != s.tp.BlockNr() {
		panic(fmt.Sprintf("blockNr %d != s.tp.BlockNr() %d", blockNr, s.tp.BlockNr()))
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	if trace {
		fmt.Printf("APPLY\n")
	}
	if len(blockProof.Masks) > 0 {
		s.t.ApplyProof(blockNr, blockProof.Masks, blockProof.ShortKeys, blockProof.Values, blockProof.Hashes, trace)
		if stateRoot != s.t.Hash() {
			filename := fmt.Sprintf("root_%d.txt", blockNr)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				s.t.Print(f)
			}
			return fmt.Errorf("[APPLY] Expected root: %x, Constructed root: %x", stateRoot, s.t.Hash())
		}
	}
	var maskIdx, hashIdx, shortIdx, valueIdx int
	for _, contract := range blockProof.Contracts {
		if trace {
			fmt.Printf("APPLY TRIE %x ==============================================\n", contract)
		}
		h.Sha.Reset()
		if _, err := h.Sha.Write(contract[:]); err != nil {
			return err
		}
		var addrHash common.Hash
		if _, err := h.Sha.Read(addrHash[:]); err != nil {
			return err
		}
		var st *trie.Trie
		var ok bool
		var mIdx, hIdx, sIdx, vIdx int
		if st, ok = s.storageTries[contract]; !ok {
			//contractCopy := contract
			touchFunc := func(hex []byte, del bool) {
				//s.tp.TouchContract(contractCopy, hex, del)
			}
			st, mIdx, hIdx, sIdx, vIdx = trie.NewFromProofs(touchFunc, blockNr, true,
				blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:], trace)
			st.SetTouchFunc(touchFunc)
			s.storageTries[contract] = st
		} else {
			mIdx, hIdx, sIdx, vIdx = st.ApplyProof(blockNr, blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:], trace)
		}
		enc, ok := s.t.Get(addrHash[:])
		if !ok {
			return fmt.Errorf("[APPLY] account %x (hash %x) is not present in the proof", contract, addrHash)
		}
		var acc accounts.Account
		if err := acc.DecodeForHashing(enc); err != nil {
			return err
		}
		if acc.Root != st.Hash() {
			filename := fmt.Sprintf("root_%d.txt", blockNr-1)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				st.Print(f)
			}
			return fmt.Errorf("[APPLY] Expected storage root for %x: %x, constructed root: %x", contract, acc.Root, st.Hash())
		}
		maskIdx += mIdx
		shortIdx += sIdx
		hashIdx += hIdx
		valueIdx += vIdx
	}
	var codeHash common.Hash
	for _, code := range blockProof.Codes {
		h.Sha.Reset()
		if _, err := h.Sha.Write(code); err != nil {
			log.Error("error on sha", "err", err)
		}
		if _, err := h.Sha.Read(codeHash[:]); err != nil {
			log.Error("error on sha", "err", err)
		}
		s.touchCodeHash(codeHash, code, blockNr)
	}
	return nil
}

func (s *Stateless) SetBlockNr(blockNr uint64) {
	s.blockNr = blockNr
	s.tp.SetBlockNr(blockNr)
}

func (s *Stateless) ReadAccountData(address common.Address) (*accounts.Account, error) {
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	if _, err := h.Sha.Write(address[:]); err != nil {
		return nil, err
	}
	var addrHash common.Hash
	if _, err := h.Sha.Read(addrHash[:]); err != nil {
		return nil, err
	}
	enc, ok := s.t.Get(addrHash[:])
	if !ok {
		return nil, fmt.Errorf("Account %x (hash %x) is not present in the proof", address, addrHash)
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForHashing(enc); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (s *Stateless) getStorageTrie(address common.Address, create bool) (*trie.Trie, error) {
	t, ok := s.storageTries[address]
	if !ok && create {
		t = trie.New(common.Hash{})
		t.SetTouchFunc(func(hex []byte, del bool) {
			//s.tp.TouchContract(address, hex, del)
		})
		s.storageTries[address] = t
	}
	return t, nil
}

func (s *Stateless) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	//fmt.Printf("ReadAccountStorage\n")
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	t, err := s.getStorageTrie(address, false)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, nil
	}
	h.Sha.Reset()
	if _, err := h.Sha.Write((*key)[:]); err != nil {
		return nil, err
	}
	var secKey common.Hash
	if _, err := h.Sha.Read(secKey[:]); err != nil {
		return nil, err
	}
	enc, ok := t.Get(secKey[:])
	if ok {
		// Unwrap one RLP level
		if len(enc) > 1 {
			enc = enc[1:]
		}
	} else {
		return nil, fmt.Errorf("Storage of %x (key %x, hash %x) is not present in the proof", address, (*key), secKey)
	}
	return common.CopyBytes(enc), nil
}

func (s *Stateless) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if codeHash == emptyCodeHashH {
		return []byte{}, nil
	}
	if codeTime, ok := s.codeMap[codeHash]; ok {
		if s.trace {
			fmt.Printf("ReadAccountCode %x: %d\n", codeHash, len(codeTime.bytecode))
		}
		s.touchCodeHash(codeHash, codeTime.bytecode, s.blockNr)
		return codeTime.bytecode, nil
	} else {
		if s.trace {
			fmt.Printf("ReadAccountCode %x: nil\n", codeHash)
		}
		return nil, fmt.Errorf("Could not find code for codeHash %x\n", codeHash)
	}
}

func (s *Stateless) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	if codeHash == emptyCodeHashH {
		return 0, nil
	}
	if codeTime, ok := s.codeMap[codeHash]; ok {
		s.touchCodeHash(codeHash, codeTime.bytecode, s.blockNr)
		return len(codeTime.bytecode), nil
	} else {
		return 0, fmt.Errorf("Could not find code for codeHash %x\n", codeHash)
	}
}

func (s *Stateless) UpdateAccountData(_ context.Context, address common.Address, original, account *accounts.Account) error {
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	if _, err := h.Sha.Write(address[:]); err != nil {
		return err
	}
	var addrHash common.Hash
	if _, err := h.Sha.Read(addrHash[:]); err != nil {
		return err
	}
	s.accountUpdates[addrHash] = account
	if s.trace {
		fmt.Printf("UpdateAccount %x, hash %x\n", address, addrHash)
	}
	return nil
}

func (s *Stateless) CheckRoot(expected common.Hash, check bool) error {
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	// Process updates first, deletes next
	for address, m := range s.storageUpdates {
		h.Sha.Reset()
		if _, err := h.Sha.Write(address[:]); err != nil {
			return err
		}
		var addrHash common.Hash
		if _, err := h.Sha.Read(addrHash[:]); err != nil {
			return err
		}
		if _, ok := s.deleted[addrHash]; ok {
			if account, ok := s.accountUpdates[addrHash]; ok && account != nil {
				account.Root = trie.EmptyRoot
			}
			storageTrie, err := s.getStorageTrie(address, false)
			if err != nil {
				return err
			}
			if storageTrie != nil {
				delete(s.storageTries, address)
			}
			continue
		}
		storageTrie, err := s.getStorageTrie(address, true)
		if err != nil {
			return err
		}
		hashes := make(Hashes, len(m))
		i := 0
		for keyHash := range m {
			hashes[i] = keyHash
			i++
		}
		sort.Sort(hashes)
		for _, keyHash := range hashes {
			v := m[keyHash]
			if len(v) != 0 {
				storageTrie.Update(keyHash[:], v, s.blockNr-1)
			} else {
				storageTrie.Delete(keyHash[:], s.blockNr-1)
			}
		}
		if account, ok := s.accountUpdates[addrHash]; ok && account != nil {
			account.Root = storageTrie.Hash()
		}
	}
	addrs := make(Hashes, len(s.accountUpdates))
	i := 0
	for addrHash := range s.accountUpdates {
		addrs[i] = addrHash
		i++
	}
	sort.Sort(addrs)
	for _, addrHash := range addrs {
		account := s.accountUpdates[addrHash]
		if account != nil {
			dataLen := account.EncodingLengthForHashing()
			data := make([]byte, dataLen)
			account.EncodeForHashing(data)
			s.t.Update(addrHash[:], data, s.blockNr-1)
		} else {
			s.t.Delete(addrHash[:], s.blockNr-1)
		}
	}
	if check {
		myRoot := s.t.Hash()
		if myRoot != expected {
			filename := fmt.Sprintf("root_%d.txt", s.blockNr)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				s.t.Print(f)
			}
			return fmt.Errorf("Final root: %x, expected: %x", myRoot, expected)
		}
	}
	s.storageUpdates = make(map[common.Address]map[common.Hash][]byte)
	s.accountUpdates = make(map[common.Hash]*accounts.Account)
	s.deleted = make(map[common.Hash]struct{})
	return nil
}

func (s *Stateless) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	//fmt.Printf("UpdateAccountCode\n")
	s.touchCodeHash(codeHash, code, s.blockNr)
	return nil
}

func (s *Stateless) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	s.accountUpdates[addrHash] = nil
	s.deleted[addrHash] = struct{}{}
	if s.trace {
		fmt.Printf("DeleteAccount %x, hash %x\n", address, addrHash)
	}
	return nil
}

func (s *Stateless) WriteAccountStorage(_ context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	m, ok := s.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		s.storageUpdates[address] = m
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
	if _, err := h.Sha.Write((*key)[:]); err != nil {
		return err
	}
	var secKey common.Hash
	if _, err := h.Sha.Read(secKey[:]); err != nil {
		return err
	}
	v := bytes.TrimLeft(value[:], "\x00")
	if len(v) > 0 {
		m[secKey] = AddExtraRLPLevel(v)
	} else {
		m[secKey] = nil
	}
	if s.trace {
		fmt.Printf("WriteAccountStorage addr %x, keyHash %x, value %x\n", address, secKey, v)
	}
	return nil
}

func (s *Stateless) Prune(oldest uint64, trace bool) {
	if trace {
		mainPrunable := s.t.CountPrunableNodes()
		prunableNodes := mainPrunable
		for _, storageTrie := range s.storageTries {
			prunableNodes += storageTrie.CountPrunableNodes()
		}
		fmt.Printf("[Before pruning to %d] Actual prunable nodes: %d (main %d), accounted: %d\n", oldest, prunableNodes, mainPrunable, s.tp.NodeCount())
	}
	//emptyAddresses, err := s.tp.PruneToTimestamp(s.t, oldest, func(contract common.Address) (*trie.Trie, error) {
	//	return s.getStorageTrie(contract, false)
	//})
	//if err != nil {
	//	fmt.Printf("Error while pruning: %v\n", err)
	//}
	if trace {
		mainPrunable := s.t.CountPrunableNodes()
		prunableNodes := mainPrunable
		for _, storageTrie := range s.storageTries {
			prunableNodes += storageTrie.CountPrunableNodes()
		}
		fmt.Printf("[After pruning to %d Actual prunable nodes: %d (main %d), accounted: %d\n", oldest, prunableNodes, mainPrunable, s.tp.NodeCount())
	}
	//for _, address := range emptyAddresses {
	//	delete(s.storageTries, address)
	//}
	if m, ok := s.timeToCodeHash[oldest-1]; ok {
		for codeHash := range m {
			if trace {
				fmt.Printf("Pruning codehash %x at time %d\n", codeHash, oldest-1)
			}
			delete(s.codeMap, codeHash)
		}
	}
	delete(s.timeToCodeHash, oldest-1)
}

func (s *Stateless) CreateContract(address common.Address) error {
	return nil
}
