package smt

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/status-im/keycard-go/hexutils"
)

// BuildWitness creates a witness from the SMT
func (s *RoSMT) BuildWitness(rd trie.RetainDecider, ctx context.Context) (*trie.Witness, error) {
	operands := make([]trie.WitnessOperator, 0)

	root, err := s.DbRo.GetLastRoot()
	if err != nil {
		return nil, err
	}

	action := func(prefix []byte, k utils.NodeKey, v utils.NodeValue12) (bool, error) {
		if rd != nil {
			/*
				This function is invoked for every node in the tree. We must decide whether to retain it or not in the witness.
				Retaining means adding node's data. Not retaining means adding only its hash.
				If a path (or rather a prefix of a path) must be retained then rd.Retain(prefix of a path) returns true. Otherwise it returns false.
				If a node (leaf or not) must be retained then rd.Retain returns true for all of the prefixes of its path => rd.Retains returns true for all node's ancestors all the way up to the root. (1)
				Therefore if a leaf must be retained then rd.Retain will return true not only for leaf's path but it will return true for all leaf's ancestors' paths.
				If a node must be retained it could be either because of a modification or because of a deletion.
				In case of modification it is enough to retain only the node but in case of deletion the witness must includes the node's sibling.
				Because of (1) if a node must be retained then its parent must be retained too => we're safe to decide whether a node must be retained or not by using its parent's parent.
				Using a parent's path will ensure that if a node is included in the witness then its sibling will also be included, because they have the same parent.
				As a result, if node must be included in the witness the code will always include its sibling.
				Using this approach we prepare the witness like we're going to deleting all node, because we actually do not have information whether a node is modified or deleted.
				This algorithm adds a little bit more nodes to the witness but it ensures that all requiring nodes are included.
			*/

			var retain bool

			prefixLen := len(prefix)
			if prefixLen > 0 {
				retain = rd.Retain(prefix[:prefixLen-1])
			} else {
				retain = rd.Retain(prefix)
			}

			if !retain {
				h := common.BigToHash(k.ToBigInt())
				hNode := trie.OperatorHash{Hash: h}
				operands = append(operands, &hNode)
				return false, nil
			}
		}

		if v.IsFinalNode() {
			actualK, err := s.DbRo.GetHashKey(k)
			if err == db.ErrNotFound {
				h := common.BigToHash(k.ToBigInt())
				hNode := trie.OperatorHash{Hash: h}
				operands = append(operands, &hNode)
				return false, nil
			} else if err != nil {
				return false, err
			}

			keySource, err := s.DbRo.GetKeySource(actualK)
			if err != nil {
				return false, err
			}

			t, addr, storage, err := utils.DecodeKeySource(keySource)
			if err != nil {
				return false, err
			}

			valHash := v.Get4to8()
			v, err := s.DbRo.Get(*valHash)
			if err != nil {
				return false, err
			}

			vInBytes := utils.ArrayBigToScalar(utils.BigIntArrayFromNodeValue8(v.GetNodeValue8())).Bytes()
			if t == utils.SC_CODE {
				code, err := s.DbRo.GetCode(vInBytes)
				if err != nil {
					return false, err
				}

				operands = append(operands, &trie.OperatorCode{Code: code})
			}

			storageKeyBytes := storage.Bytes()
			if t != utils.SC_STORAGE {
				storageKeyBytes = []byte{}
			}
			// fmt.Printf("Node hash: %s, Node type: %d, address %x, storage %x, value %x\n", utils.ConvertBigIntToHex(k.ToBigInt()), t, addr, storage, utils.ArrayBigToScalar(value8).Bytes())
			operands = append(operands, &trie.OperatorSMTLeafValue{
				NodeType:   uint8(t),
				Address:    addr.Bytes(),
				StorageKey: storageKeyBytes,
				Value:      vInBytes,
			})
			return false, nil
		}

		var mask uint32
		if !v.Get0to4().IsZero() {
			mask |= 1
		}

		if !v.Get4to8().IsZero() {
			mask |= 2
		}

		operands = append(operands, &trie.OperatorBranch{
			Mask: mask,
		})

		return true, nil
	}

	err = s.Traverse(ctx, root, action)

	return trie.NewWitness(operands), err
}

// BuildSMTfromWitness builds SMT from witness
func BuildSMTFromWitness(w *trie.Witness) (*SMT, error) {
	// using memdb
	s := NewSMT(nil, false)

	if err := AddWitnessToSMT(s, w); err != nil {
		return nil, fmt.Errorf("AddWitnessToSMT: %w", err)
	}

	return s, nil
}

func AddWitnessToSMT(s *SMT, w *trie.Witness) error {
	balanceMap := make(map[string]*big.Int)
	nonceMap := make(map[string]*big.Int)
	contractMap := make(map[string]string)
	storageMap := make(map[string]map[string]string)

	path := make([]int, 0)

	firstNode := true
	NodeChildCountMap := make(map[string]uint32)
	NodesBranchValueMap := make(map[string]uint32)

	type nodeHash struct {
		path []int
		hash common.Hash
	}

	nodeHashes := make([]nodeHash, 0)

	for i, operator := range w.Operators {
		switch op := operator.(type) {
		case *trie.OperatorSMTLeafValue:
			valScaler := big.NewInt(0).SetBytes(op.Value)
			addr := common.BytesToAddress(op.Address)
			switch op.NodeType {
			case utils.KEY_BALANCE:
				balanceMap[addr.String()] = valScaler

			case utils.KEY_NONCE:
				nonceMap[addr.String()] = valScaler

			case utils.SC_STORAGE:
				if _, ok := storageMap[addr.String()]; !ok {
					storageMap[addr.String()] = make(map[string]string)
				}

				stKey := hexutils.BytesToHex(op.StorageKey)
				if len(stKey) > 0 {
					stKey = fmt.Sprintf("0x%s", stKey)
				}

				storageMap[addr.String()][stKey] = valScaler.String()
			}
			path = path[:len(path)-1]
			NodeChildCountMap[intArrayToString(path)] += 1

			for len(path) != 0 && NodeChildCountMap[intArrayToString(path)] == NodesBranchValueMap[intArrayToString(path)] {
				path = path[:len(path)-1]
			}
			if NodeChildCountMap[intArrayToString(path)] < NodesBranchValueMap[intArrayToString(path)] {
				path = append(path, 1)
			}

		case *trie.OperatorCode:
			addr := common.BytesToAddress(w.Operators[i+1].(*trie.OperatorSMTLeafValue).Address)

			code := hexutils.BytesToHex(op.Code)
			if len(code) > 0 {
				if err := s.Db.AddCode(hexutils.HexToBytes(code)); err != nil {
					return err
				}
				code = fmt.Sprintf("0x%s", code)
			}

			contractMap[addr.String()] = code

		case *trie.OperatorBranch:
			if firstNode {
				firstNode = false
			} else {
				NodeChildCountMap[intArrayToString(path[:len(path)-1])] += 1
			}

			switch op.Mask {
			case 1:
				NodesBranchValueMap[intArrayToString(path)] = 1
				path = append(path, 0)
			case 2:
				NodesBranchValueMap[intArrayToString(path)] = 1
				path = append(path, 1)
			case 3:
				NodesBranchValueMap[intArrayToString(path)] = 2
				path = append(path, 0)
			}

		case *trie.OperatorHash:
			pathCopy := make([]int, len(path))
			copy(pathCopy, path)
			nodeHashes = append(nodeHashes, nodeHash{path: pathCopy, hash: op.Hash})
			path = path[:len(path)-1]
			NodeChildCountMap[intArrayToString(path)] += 1

			for len(path) != 0 && NodeChildCountMap[intArrayToString(path)] == NodesBranchValueMap[intArrayToString(path)] {
				path = path[:len(path)-1]
			}
			if NodeChildCountMap[intArrayToString(path)] < NodesBranchValueMap[intArrayToString(path)] {
				path = append(path, 1)
			}

		default:
			// Unsupported operator type
			return fmt.Errorf("unsupported operator type: %T", op)
		}
	}

	for _, nodeHash := range nodeHashes {
		// should not replace with hash node if there are nodes under it on the current smt
		// we would lose needed data i we replace it with a hash node
		node, err := s.GetNodeAtPath(nodeHash.path)
		if err != nil {
			return fmt.Errorf("GetNodeAtPath: %w", err)
		}
		if node != nil {
			continue
		}
		if _, err := s.InsertHashNode(nodeHash.path, nodeHash.hash.Big()); err != nil {
			return fmt.Errorf("InsertHashNode: %w", err)
		}

		if _, err = s.Db.GetLastRoot(); err != nil {
			return fmt.Errorf("GetLastRoot: %w", err)
		}
	}

	for addr, balance := range balanceMap {
		if _, err := s.SetAccountBalance(addr, balance); err != nil {
			return fmt.Errorf("SetAccountBalance: %w", err)
		}
	}

	for addr, nonce := range nonceMap {
		if _, err := s.SetAccountNonce(addr, nonce); err != nil {
			return fmt.Errorf("SetAccountNonce: %w", err)
		}
	}

	for addr, code := range contractMap {
		if err := s.SetContractBytecode(addr, code); err != nil {
			return fmt.Errorf("SetContractBytecode: %w", err)
		}
	}

	for addr, storage := range storageMap {
		if _, err := s.SetContractStorage(addr, storage, nil); err != nil {
			return fmt.Errorf("SetContractStorage: %w", err)
		}
	}

	return nil
}
