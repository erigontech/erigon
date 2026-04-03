package qmtree

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
)

type ProofNode struct {
	SelfHash   common.Hash
	PeerHash   common.Hash
	PeerAtLeft bool
}

// ProofPath contains the Merkle proof for a single leaf.
// With activeBits removed, the proof is:
//   - LeftOfTwig [11]ProofNode: path through the twig's leaf-hash Merkle tree
//   - UpperPath  []ProofNode:   path through the upper (inter-twig) tree
//   - Root:                     the tree root at the time of proof
type ProofPath struct {
	LeftOfTwig [11]ProofNode
	UpperPath  []ProofNode
	TxNum  uint64
	Root       common.Hash
}

// OTHER_NODE_COUNT: 1 (LeftOfTwig[0].SelfHash) + 11 (LeftOfTwig PeerHashes) + 1 (Root)
const OTHER_NODE_COUNT = 1 + 11 + 1

func (p *ProofPath) ToBytes() []byte {
	res := make([]byte, 0, (8+(len(p.UpperPath)+OTHER_NODE_COUNT)*32))
	res = binary.LittleEndian.AppendUint64(res, p.TxNum) // 8-byte
	res = append(res, p.LeftOfTwig[0].SelfHash[:]...)        // 1
	for i := range p.LeftOfTwig {
		//11
		res = append(res, p.LeftOfTwig[i].PeerHash[:]...)
	}
	for i := range p.UpperPath {
		res = append(res, p.UpperPath[i].PeerHash[:]...)
	}
	res = append(res, p.Root[:]...) //1
	return res
}

func (p *ProofPath) Check(hasher Hasher, complete bool) error {
	// levels 0..9: each level's hash feeds into the next level's SelfHash
	for i := range 10 {
		res := hasher.hash2x(
			uint8(i),
			p.LeftOfTwig[i].SelfHash[:],
			p.LeftOfTwig[i].PeerHash[:],
			p.LeftOfTwig[i].PeerAtLeft,
		)

		if complete {
			p.LeftOfTwig[i+1].SelfHash = res
		} else if res != p.LeftOfTwig[i+1].SelfHash {
			return fmt.Errorf("mismatch at left path, level: %d", i)
		}
	}

	// level 10: produces the twig root (= left-subtree root, activeBits removed)
	twigRoot := hasher.hash2x(
		10,
		p.LeftOfTwig[10].SelfHash[:],
		p.LeftOfTwig[10].PeerHash[:],
		p.LeftOfTwig[10].PeerAtLeft,
	)

	if complete {
		p.UpperPath[0].SelfHash = twigRoot
	} else if twigRoot != p.UpperPath[0].SelfHash {
		return errors.New("mismatch at twig top")
	}

	for i := range p.UpperPath {
		level := TwigRootLevel + i
		res := hasher.hash2x(
			uint8(level),
			p.UpperPath[i].SelfHash[:],
			p.UpperPath[i].PeerHash[:],
			p.UpperPath[i].PeerAtLeft,
		)

		if i < len(p.UpperPath)-1 {
			if complete {
				p.UpperPath[i+1].SelfHash = res
			} else if res != p.UpperPath[i+1].SelfHash {
				return fmt.Errorf("mismatch at upper path, level: %d", level)
			}
		} else if res != p.Root {
			return errors.New("mismatch at root")
		}
	}

	return nil
}

func BytesToProofPath(bz []byte) (*ProofPath, error) {
	n := len(bz) - 8
	upperCount := (n/32 - OTHER_NODE_COUNT)
	if n%32 != 0 || upperCount < 0 {
		return nil, fmt.Errorf("Invalid byte slice length: %d", len(bz))
	}
	upperPath := make([]ProofNode, 0, upperCount)
	emptyNode := ProofNode{}
	leftOfTwig := [11]ProofNode{}
	txNum := binary.LittleEndian.Uint64(bz[0:8])
	bz = bz[8:]
	copy(leftOfTwig[0].SelfHash[:], bz[:32])
	bz = bz[32:]
	for i := range leftOfTwig {
		copy(leftOfTwig[i].PeerHash[:], bz[:32])
		leftOfTwig[i].PeerAtLeft = (txNum>>i)&1 == 1
		bz = bz[32:]
	}
	for i := range upperCount {
		node := emptyNode
		copy(node.PeerHash[:], bz[:32])
		node.PeerAtLeft = ((txNum >> (FIRST_LEVEL_ABOVE_TWIG - 2 + i)) & 1) == 1
		upperPath = append(upperPath, node)
		bz = bz[32:]
	}
	root := common.Hash{}
	copy(root[:], bz[:32])
	return &ProofPath{
		leftOfTwig,
		upperPath,
		txNum,
		root,
	}, nil
}

func CheckProof(hasher Hasher, path *ProofPath) ([]byte, error) {
	err := path.Check(hasher, false)
	if err != nil {
		return nil, err
	}
	bz := path.ToBytes()
	path2, err := BytesToProofPath(bz)
	if err != nil {
		return nil, err
	}
	err = path2.Check(hasher, true)
	if err != nil {
		return nil, err
	}
	return bz, nil
}

func GetLeftPath(sn uint64, getHash func(uint64) common.Hash) [11]ProofNode {
	n := sn & TWIG_MASK
	left := [11]ProofNode{}
	for level := range 11 {
		stride := uint64(2048 >> level)
		selfId := (n >> level)
		peer := selfId ^ 1
		left[level].SelfHash = getHash(uint64(stride + selfId))
		left[level].PeerHash = getHash(stride + peer)
		left[level].PeerAtLeft = peer&1 == 0
	}
	return left
}

func GetLeftPathInMem(mt4twig TwigMT, sn uint64) [11]ProofNode {
	return GetLeftPath(sn, func(i uint64) common.Hash { return mt4twig[i] })
}

func GetLeftPathOnDisk(tf TwigStorage, twig_id uint64, sn uint64) [11]ProofNode {
	cache := map[uint64]common.Hash{}
	return GetLeftPath(sn, func(i uint64) common.Hash {
		if v, ok := cache[i]; ok {
			return v
		}
		h, err := tf.GetHashNode(twig_id, i, cache)
		if err != nil {
			panic(fmt.Sprintf("GetHashNode twig=%d idx=%d: %v", twig_id, i, err))
		}
		return h
	})
}
