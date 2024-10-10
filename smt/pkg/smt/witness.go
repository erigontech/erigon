package smt

import (
	"context"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

func BuildWitness(s *SMT, rd trie.RetainDecider, ctx context.Context) (*trie.Witness, error) {
	operands := make([]trie.WitnessOperator, 0)

	root, err := s.Db.GetLastRoot()
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

			retain := true

			prefixLen := len(prefix)
			if prefixLen > 0 {
				retain = rd.Retain(prefix[:prefixLen-1])
			} else {
				retain = rd.Retain(prefix)
			}

			if !retain {
				h := libcommon.BigToHash(k.ToBigInt())
				hNode := trie.OperatorHash{Hash: h}
				operands = append(operands, &hNode)
				return false, nil
			}
		}

		if v.IsFinalNode() {
			actualK, err := s.Db.GetHashKey(k)
			if err != nil {
				return false, err
			}

			keySource, err := s.Db.GetKeySource(actualK)
			if err != nil {
				return false, err
			}

			t, addr, storage, err := utils.DecodeKeySource(keySource)
			if err != nil {
				return false, err
			}

			valHash := v.Get4to8()
			v, err := s.Db.Get(*valHash)
			if err != nil {
				return false, err
			}

			vInBytes := utils.ArrayBigToScalar(utils.BigIntArrayFromNodeValue8(v.GetNodeValue8())).Bytes()
			if t == utils.SC_CODE {
				code, err := s.Db.GetCode(vInBytes)
				if err != nil {
					return false, err
				}

				operands = append(operands, &trie.OperatorCode{Code: code})
			}

			// fmt.Printf("Node hash: %s, Node type: %d, address %x, storage %x, value %x\n", utils.ConvertBigIntToHex(k.ToBigInt()), t, addr, storage, utils.ArrayBigToScalar(value8).Bytes())
			operands = append(operands, &trie.OperatorSMTLeafValue{
				NodeType:   uint8(t),
				Address:    addr.Bytes(),
				StorageKey: storage.Bytes(),
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
