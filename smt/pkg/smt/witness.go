package smt

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
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
		if rd != nil && !rd.Retain(prefix) {
			if !v.IsFinalNode() {
				h := libcommon.BigToHash(k.ToBigInt())
				hNode := trie.OperatorHash{Hash: h}
				operands = append(operands, &hNode)
			}
			return false, nil
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

			value, err := s.Db.Get(*valHash)

			value8 := utils.BigIntArrayFromNodeValue8(value.GetNodeValue8())

			if err != nil {
				return false, err
			}

			// fmt.Printf("Node hash: %s, Node type: %d, address %x, storage %x, value %x\n", utils.ConvertBigIntToHex(k.ToBigInt()), t, addr, storage, utils.ArrayBigToScalar(value8).Bytes())

			operands = append(operands, &trie.OperatorSMTLeafValue{
				NodeType:   uint8(t),
				Address:    addr.Bytes(),
				StorageKey: storage.Bytes(),
				Value:      utils.ArrayBigToScalar(value8).Bytes(),
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
