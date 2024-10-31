package trie

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-verkle"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vkutils"
	"github.com/ledgerwatch/log/v3"
)

var (
	errInvalidRootType    = errors.New("invalid node type for root")
	errDeletedAccount     = errors.New("account deleted in VKT")
	zero                  [32]byte
	VerkleDBPathKeyPrefix = []byte("Path-")
)

type VerkleTrie struct {
	root       verkle.VerkleNode
	db         kv.RwDB
	tx         kv.RwTx
	pointCache *vkutils.PointCache
	ended      bool
}

func (vt *VerkleTrie) ToDot() string {
	return verkle.ToDot(vt.root)
}

func NewVerkleTrie(root verkle.VerkleNode, db kv.RwDB, tx kv.RwTx, pointCache *vkutils.PointCache, ended bool) *VerkleTrie {
	return &VerkleTrie{
		root:       root,
		db:         db,
		tx:         tx,
		pointCache: pointCache,
		ended:      ended,
	}
}

func OpenVKTrie(root common.Hash, tx kv.RwTx) (*VerkleTrie, error) {
	// Root is stored at VerkleDBPathKeyPrefix
	payload, err := tx.GetOne(kv.VerkleTrie, VerkleDBPathKeyPrefix)
	if err != nil || len(payload) == 0 {
		return NewVerkleTrie(verkle.New(), nil, tx, vkutils.NewPointCache(), false), nil
	}

	r, err := verkle.ParseNode(payload, 0)
	if err != nil {
		panic(err)
	}
	return NewVerkleTrie(r, nil, tx, vkutils.NewPointCache(), false), err
}

func (vt *VerkleTrie) DbNodeResolver(path []byte) ([]byte, error) {
	fullPath := make([]byte, 0, len(VerkleDBPathKeyPrefix)+32)
	fullPath = append(fullPath, VerkleDBPathKeyPrefix...)
	fullPath = append(fullPath[:len(VerkleDBPathKeyPrefix)], path...)

	if vt.tx != nil {
		return vt.tx.GetOne(kv.VerkleTrie, fullPath)
	}
	tx, err := vt.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return tx.GetOne(kv.VerkleTrie, fullPath)
}

// Get returns the value for key stored in the vt. The value bytes must
// not be modified by the caller. If a node was not found in the database, a
// vt.MissingNodeError is returned.
func (vt *VerkleTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	pointEval := vt.pointCache.GetTreeKeyHeader(addr[:])
	k := vkutils.GetTreeKeyStorageSlotWithEvaluatedAddress(pointEval, key)
	return vt.root.Get(k, vt.DbNodeResolver)
}

// GetWithHashedKey returns the value, assuming that the key has already
// been hashed.
func (vt *VerkleTrie) GetWithHashedKey(key []byte) ([]byte, error) {
	return vt.root.Get(key, vt.DbNodeResolver)
}

func (t *VerkleTrie) GetAccount(addr common.Address) (*accounts.Account, error) {
	acc := &accounts.Account{}
	versionkey := t.pointCache.GetTreeKeyVersionCached(addr[:])
	var (
		values [][]byte
		err    error
	)
	switch t.root.(type) {
	case *verkle.InternalNode:
		values, err = t.root.(*verkle.InternalNode).GetValuesAtStem(versionkey[:31], t.DbNodeResolver)
	default:
		return nil, errInvalidRootType
	}
	if err != nil {
		return nil, fmt.Errorf("GetAccount (%x) error: %v", addr, err)
	}

	// emptyAccount := true
	// for i := 0; values != nil && i <= vkutils.CodeHashLeafKey && emptyAccount; i++ {
		// emptyAccount = emptyAccount && values[i] == nil
	// }
	// if emptyAccount {
		// return nil, nil
	// }
	if len(values[vkutils.NonceLeafKey]) > 0 {
		acc.Nonce = binary.LittleEndian.Uint64(values[vkutils.NonceLeafKey])
	}
	// if the account has been deleted, then values[10] will be 0 and not nil. If it has
	// been recreated after that, then its code keccak will NOT be 0. So return `nil` if
	// the nonce, and values[10], and code keccak is 0.

	if acc.Nonce == 0 && len(values) > 10 && len(values[10]) > 0 && bytes.Equal(values[vkutils.CodeHashLeafKey], zero[:]) {
		if !t.ended {
			return nil, errDeletedAccount
		} else {
			return nil, nil
		}
	}
	var balance [32]byte
	copy(balance[:], values[vkutils.BalanceLeafKey])
	for i := 0; i < len(balance)/2; i++ {
		balance[len(balance)-i-1], balance[i] = balance[i], balance[len(balance)-i-1]
	}
	// var balance [32]byte
	// if len(values[utils.BalanceLeafKey]) > 0 {
	// 	for i := 0; i < len(balance); i++ {
	// 		balance[len(balance)-i-1] = values[utils.BalanceLeafKey][i]
	// 	}
	// }
	acc.Balance = *uint256.NewInt(0).SetBytes(balance[:])
	acc.CodeHash = common.BytesToHash(values[vkutils.CodeHashLeafKey])
	// TODO fix the code size as well

	return acc, nil
}

func (t *VerkleTrie) UpdateAccount(addr common.Address, acc *accounts.Account) error {
	var (
		err            error
		nonce, balance [32]byte
		values         = make([][]byte, verkle.NodeWidth)
		stem           = t.pointCache.GetTreeKeyVersionCached(addr[:])
	)

	if addr == [20]byte{ 224,122,101,136,176,109,180,145,163,22,33,147,25,166,86,6,3,128,86,66} {
		_ = 20
		log.Info("Nada")
	}

	// Only evaluate the polynomial once
	values[vkutils.VersionLeafKey] = zero[:]
	values[vkutils.NonceLeafKey] = nonce[:]
	values[vkutils.BalanceLeafKey] = balance[:]
	values[vkutils.CodeHashLeafKey] = acc.CodeHash[:]

	binary.LittleEndian.PutUint64(nonce[:], acc.Nonce)
	bbytes := acc.Balance.Bytes()
	if len(bbytes) > 0 {
		for i, b := range bbytes {
			balance[len(bbytes)-i-1] = b
		}
	}

	switch root := t.root.(type) {
	case *verkle.InternalNode:
		err = root.InsertValuesAtStem(stem, values, t.DbNodeResolver)
	default:
		return errInvalidRootType
	}
	if err != nil {
		log.Error("[SPIDERMAN] Error in UpdateAccount", "error", err, "addr", addr)
		return fmt.Errorf("UpdateAccount (%x) error: %v", addr, err)
	}
	// TODO figure out if the code size needs to be updated, too

	return nil
}

func (vt *VerkleTrie) UpdateStem(key []byte, values [][]byte) error {
	switch root := vt.root.(type) {
	case *verkle.InternalNode:
		return root.InsertValuesAtStem(key, values, vt.DbNodeResolver)
	default:
		panic("invalid root type")
	}
}

// Update associates key with value in the vt. If value has length zero, any
// existing value is deleted from the vt. The value bytes must not be modified
// by the caller while they are stored in the vt. If a node was not found in the
// database, a vt.MissingNodeError is returned.
func (vt *VerkleTrie) UpdateStorage(address common.Address, key, value []byte) error {
	k := vkutils.GetTreeKeyStorageSlotWithEvaluatedAddress(vt.pointCache.GetTreeKeyHeader(address[:]), key)
	var v [32]byte
	if len(value) >= 32 {
		copy(v[:], value[:32])
	} else {
		copy(v[32-len(value):], value[:])
	}
	return vt.root.Insert(k, v[:], vt.DbNodeResolver)
}

func (t *VerkleTrie) DeleteAccount(addr common.Address) error {
	// var (
	// 	err    error
	// 	values = make([][]byte, verkle.NodeWidth)
	// 	stem   = t.pointCache.GetTreeKeyVersionCached(addr[:])
	// )

	// for i := 0; i < verkle.NodeWidth; i++ {
	// 	values[i] = zero[:]
	// }

	// switch root := t.root.(type) {
	// case *verkle.InternalNode:
	// 	err = root.InsertValuesAtStem(stem, values, t.DbNodeResolver)
	// default:
	// 	return errInvalidRootType
	// }
	// if err != nil {
	// 	return fmt.Errorf("DeleteAccount (%x) error: %v", addr, err)
	// }
	// TODO figure out if the code size needs to be updated, too

	return nil
}

// Delete removes any existing value for key from the vt. If a node was not
// found in the database, a vt.MissingNodeError is returned.
func (vt *VerkleTrie) DeleteStorage(addr common.Address, key []byte) error {
	pointEval := vt.pointCache.GetTreeKeyHeader(addr[:])
	k := vkutils.GetTreeKeyStorageSlotWithEvaluatedAddress(pointEval, key)
	var zero [32]byte
	return vt.root.Insert(k, zero[:], vt.DbNodeResolver)
}

// Hash returns the root hash of the vt. It does not write to the database and
// can be used even if the trie doesn't have one.
func (vt *VerkleTrie) Hash() common.Hash {
	return vt.root.Commit().Bytes()
}

// NOT USED
func nodeToDBKey(n verkle.VerkleNode) []byte {
	ret := n.Commitment().Bytes()
	return ret[:]
}

// Commit writes all nodes to the trie's memory database, tracking the internal
// and external (for account tries) references.
func (vt *VerkleTrie) Commit(_ bool) (common.Hash, error) {
	root, ok := vt.root.(*verkle.InternalNode)
	if !ok {
		return common.Hash{}, errors.New(fmt.Sprintf("unexpected root node type %v", root.Hash().String()))
	}

	if vt.tx == nil {
		if vt.db == nil {
			return vt.Hash(), nil
		}
		var err error
		vt.tx, err = vt.db.BeginRw(context.Background())
		if err != nil {
			return common.Hash{}, err
		}
		defer vt.tx.Rollback()
	}

	nodes, err := root.BatchSerialize()
	path := make([]byte, 0, len(VerkleDBPathKeyPrefix)+32)
	path = append(path, VerkleDBPathKeyPrefix...)
	if err != nil {
		return common.Hash{}, fmt.Errorf("serializing tree nodes: %s", err)
	}
	for _, node := range nodes {
		path := append(path[:len(VerkleDBPathKeyPrefix)], node.Path...)
		if err := vt.tx.Put(kv.VerkleTrie, path, node.SerializedBytes); err != nil {
			return common.Hash{}, fmt.Errorf("put node to disk: %s", err)
		}
	}

	if vt.db != nil {
		err = vt.tx.Commit()
		if err != nil {
			return common.Hash{}, err
		}
	}
	// var err error
	// root.Flush(func(path []byte, node verkle.VerkleNode) {
	// 	var s []byte
	// 	s, err = node.Serialize()
	// 	if err != nil {
	// 		return
	// 	}
	// 	if err = vt.tx.Put(kv.VerkleTrie, path, s); err != nil {
	// 		return
	// 	}
	// })

	return vt.Hash(), nil
}

// // NodeIterator returns an iterator that returns nodes of the vt. Iteration
// // starts at the key after the given start key.
// func (vt *VerkleTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
// 	return newVerkleNodeIterator(trie, nil)
// }

// Prove constructs a Merkle proof for key. The result contains all encoded nodes
// on the path to the value at key. The value itself is also included in the last
// node and can be retrieved by verifying the proof.
//
// If the trie does not contain a value for key, the returned proof contains all
// nodes of the longest existing prefix of the key (at least the root), ending
// with the node that proves the absence of the key.
// func (vt *VerkleTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
// 	panic("not implemented")
// }

func (vt *VerkleTrie) Copy() *VerkleTrie {
	return &VerkleTrie{
		root:       vt.root.Copy(),
		db:         vt.db,
		pointCache: vt.pointCache,
	}
}

func (vt *VerkleTrie) IsVerkle() bool {
	return true
}

func ProveAndSerialize(pretrie, posttrie *VerkleTrie, keys [][]byte, resolver verkle.NodeResolverFn) (*verkle.VerkleProof, verkle.StateDiff, error) {
	var postroot verkle.VerkleNode
	if posttrie != nil {
		postroot = posttrie.root
	}
	proof, _, _, _, err := verkle.MakeVerkleMultiProof(pretrie.root, postroot, keys, resolver)
	if err != nil {
		return nil, nil, err
	}

	p, kvps, err := verkle.SerializeProof(proof)
	if err != nil {
		return nil, nil, err
	}

	return p, kvps, nil
}

func DeserializeAndVerifyVerkleProof(vp *verkle.VerkleProof, preStateRoot []byte, postStateRoot []byte, statediff verkle.StateDiff) error {
	// TODO: check that `OtherStems` have expected length and values.

	proof, err := verkle.DeserializeProof(vp, statediff)
	if err != nil {
		return fmt.Errorf("verkle proof deserialization error: %w", err)
	}

	rootC := new(verkle.Point)
	rootC.SetBytes(preStateRoot)
	pretree, err := verkle.PreStateTreeFromProof(proof, rootC)
	if err != nil {
		return fmt.Errorf("error rebuilding the pre-tree from proof: %w", err)
	}
	// TODO this should not be necessary, remove it
	// after the new proof generation code has stabilized.
	for _, stemdiff := range statediff {
		for _, suffixdiff := range stemdiff.SuffixDiffs {
			var key [32]byte
			copy(key[:31], stemdiff.Stem[:])
			key[31] = suffixdiff.Suffix

			val, err := pretree.Get(key[:], nil)
			if err != nil {
				return fmt.Errorf("could not find key %x in tree rebuilt from proof: %w", key, err)
			}
			if len(val) > 0 {
				if !bytes.Equal(val, suffixdiff.CurrentValue[:]) {
					return fmt.Errorf("could not find correct value at %x in tree rebuilt from proof: %x != %x", key, val, *suffixdiff.CurrentValue)
				}
			} else {
				if suffixdiff.CurrentValue != nil && len(suffixdiff.CurrentValue) != 0 {
					return fmt.Errorf("could not find correct value at %x in tree rebuilt from proof: %x != %x", key, val, *suffixdiff.CurrentValue)
				}
			}
		}
	}

	// TODO: this is necessary to verify that the post-values are the correct ones.
	// But all this can be avoided with a even faster way. The EVM block execution can
	// keep track of the written keys, and compare that list with this post-values list.
	// This can avoid regenerating the post-tree which is somewhat expensive.
	posttree, err := verkle.PostStateTreeFromStateDiff(pretree, statediff)
	if err != nil {
		return fmt.Errorf("error rebuilding the post-tree from proof: %w", err)
	}
	regeneratedPostTreeRoot := posttree.Commitment().Bytes()
	if !bytes.Equal(regeneratedPostTreeRoot[:], postStateRoot) {
		return fmt.Errorf("post tree root mismatch: %x != %x", regeneratedPostTreeRoot, postStateRoot)
	}

	return verkle.VerifyVerkleProofWithPreState(proof, pretree)
}

// func (t *VerkleTrie) SetStorageRootConversion(addr common.Address, root common.Hash) {
// 	t.db.SetStorageRootConversion(addr, root)
// }

// func (t *VerkleTrie) ClearStrorageRootConversion(addr common.Address) {
// 	t.db.ClearStorageRootConversion(addr)
// }

func (t *VerkleTrie) UpdateContractCode(addr common.Address, codeHash common.Hash, code []byte) error {
	var (
		chunks = vkutils.ChunkifyCode(code)
		values [][]byte
		key    []byte
		err    error
	)
	for i, chunknr := 0, uint64(0); i < len(chunks); i, chunknr = i+32, chunknr+1 {
		groupOffset := (chunknr + 128) % 256
		if groupOffset == 0 /* start of new group */ || chunknr == 0 /* first chunk in header group */ {
			values = make([][]byte, verkle.NodeWidth)
			key = vkutils.GetTreeKeyCodeChunkWithEvaluatedAddress(t.pointCache.GetTreeKeyHeader(addr[:]), uint256.NewInt(chunknr))
		}
		values[groupOffset] = chunks[i : i+32]

		// Reuse the calculated key to also update the code size.
		if i == 0 {
			cs := make([]byte, 32)
			binary.LittleEndian.PutUint64(cs, uint64(len(code)))
			values[vkutils.CodeSizeLeafKey] = cs
		}

		if groupOffset == 255 || len(chunks)-i <= 32 {
			err = t.UpdateStem(key[:31], values)

			if err != nil {
				return fmt.Errorf("UpdateContractCode (addr=%x) error: %w", addr[:], err)
			}
		}
	}
	return nil
}
