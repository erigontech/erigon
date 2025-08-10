package qmtree

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
)

type ProofNode struct {
	SelfHash   common.Hash
	PeerHash   common.Hash
	PeerAtLeft bool
}

type ProofPath struct {
	LeftOfTwig  [11]ProofNode
	RightOfTwig [3]ProofNode
	UpperPath   []ProofNode
	SerialNum   uint64
	Root        common.Hash
}

const OTHER_NODE_COUNT = 1 + 11 + 1 + 3 + 1

func (p *ProofPath) ToBytes() []byte {
	res := make([]byte, 0, (8 + (len(p.UpperPath)+OTHER_NODE_COUNT)*32))
	res = binary.LittleEndian.AppendUint64(res, p.SerialNum) // 8-byte
	res = append(res, p.LeftOfTwig[0].SelfHash[:]...)        // 1
	for i := range p.LeftOfTwig {
		//11
		res = append(res, p.LeftOfTwig[i].PeerHash[:]...)
	}
	res = append(res, p.RightOfTwig[0].SelfHash[:]...) //1
	for i := range p.RightOfTwig {
		//3
		res = append(res, p.RightOfTwig[i].PeerHash[:]...)
	}
	for i := range p.UpperPath {
		res = append(res, p.UpperPath[i].PeerHash[:]...)
	}
	res = append(res, p.Root[:]...) //1
	return res
}

func (p *ProofPath) Check(hasher Hasher, complete bool) error {
	for i := range p.LeftOfTwig {
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

	leaf_mt_root := hasher.hash2x(
		10,
		p.LeftOfTwig[10].SelfHash[:],
		p.LeftOfTwig[10].PeerHash[:],
		p.LeftOfTwig[10].PeerAtLeft,
	)

	for i := range 2 {
		res := hasher.hash2x(
			uint8((i + 8)),
			p.RightOfTwig[i].SelfHash[:],
			p.RightOfTwig[i].PeerHash[:],
			p.RightOfTwig[i].PeerAtLeft,
		)
		if complete {
			p.RightOfTwig[i+1].SelfHash = res
		} else if res != p.RightOfTwig[i+1].SelfHash {
			return fmt.Errorf("mismatch at right path, level: %d", i)
		}
	}

	active_bits_mt_l3 := hasher.hash2x(
		10,
		p.RightOfTwig[2].SelfHash[:],
		p.RightOfTwig[2].PeerHash[:],
		p.RightOfTwig[2].PeerAtLeft,
	)

	twigRoot := hasher.hash2(11, leaf_mt_root[:], active_bits_mt_l3[:])
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
	rightOfTwig := [3]ProofNode{}
	serialNum := binary.LittleEndian.Uint64(bz[0:8])
	bz = bz[8:]
	copy(leftOfTwig[0].SelfHash[:], bz[:32])
	bz = bz[32:]
	for i := range leftOfTwig {
		copy(leftOfTwig[i].PeerHash[:], bz[:32])
		leftOfTwig[i].PeerAtLeft = (serialNum>>i)&1 == 1
		bz = bz[32:]
	}
	copy(rightOfTwig[0].SelfHash[:], bz[:32])
	bz = bz[32:]
	for i := range rightOfTwig {
		copy(rightOfTwig[i].PeerHash[:], bz[:32])
		rightOfTwig[i].PeerAtLeft = (serialNum>>(8+i))&1 == 1
		bz = bz[32:]
	}
	for i := range upperCount {
		node := emptyNode
		copy(node.PeerHash[:], bz[:32])
		node.PeerAtLeft = ((serialNum >> (FIRST_LEVEL_ABOVE_TWIG - 2 + i)) & 1) == 1
		upperPath = append(upperPath, node)
		bz = bz[32:]
	}
	root := common.Hash{}
	copy(root[:], bz[:32])
	return &ProofPath{
		leftOfTwig,
		rightOfTwig,
		upperPath,
		serialNum,
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

func GetRightPath(twig *Twig, active_bits *ActiveBits, sn uint64) [3]ProofNode {
	n := sn & TWIG_MASK
	right := [3]ProofNode{}
	selfId := n / 256
	peer := selfId ^ 1
	copy(right[0].SelfHash[:], active_bits.GetBits(int(selfId), 32))
	copy(right[0].PeerHash[:], active_bits.GetBits(int(peer), 32))
	right[0].PeerAtLeft = (peer & 1) == 0

	selfId = n / 512
	peer = selfId ^ 1
	right[1].SelfHash = twig.activeBitsMtl1[selfId]
	right[1].PeerHash = twig.activeBitsMtl1[peer]
	right[1].PeerAtLeft = (peer & 1) == 0

	selfId = n / 1024
	peer = selfId ^ 1
	right[2].SelfHash = twig.activeBitsMtl2[selfId]
	right[2].PeerHash = twig.activeBitsMtl2[peer]
	right[2].PeerAtLeft = (peer & 1) == 0
	return right
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
	cache := map[int64]common.Hash{}
	return GetLeftPath(sn, func(i uint64) common.Hash {
		if v, ok := cache[int64(i)]; ok {
			return v
		} else {
			return tf.GetHashNode(twig_id, i, cache)
		}
	})
}
