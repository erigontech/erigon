package rawdb

import (
	"github.com/ethereum/go-verkle"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
)

func ReadVerkleRoot(tx kv.Tx, blockNum uint64) (common.Hash, error) {
	root, err := tx.GetOne(kv.VerkleRoots, hexutility.EncodeTs(blockNum))
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(root), nil
}

func WriteVerkleRoot(tx kv.RwTx, blockNum uint64, root common.Hash) error {
	return tx.Put(kv.VerkleRoots, hexutility.EncodeTs(blockNum), root[:])
}

func WriteVerkleNode(tx kv.RwTx, node verkle.VerkleNode) error {
	var (
		root    common.Hash
		encoded []byte
		err     error
	)
	root = node.Commitment().Bytes()
	encoded, err = node.Serialize()
	if err != nil {
		return err
	}

	return tx.Put(kv.VerkleTrie, root[:], encoded)
}

func ReadVerkleNode(tx kv.RwTx, root common.Hash) (verkle.VerkleNode, error) {
	encoded, err := tx.GetOne(kv.VerkleTrie, root[:])
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return verkle.New(), nil
	}
	return verkle.ParseNode(encoded, 0) // Todo @somnathb1: put in the correct value of depth of the node
}
