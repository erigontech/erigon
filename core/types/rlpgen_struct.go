package types

import (
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"
)

type TestingStruct struct {
	a  uint64
	aa *uint64
	b  big.Int
	bb *big.Int
	c  uint256.Int
	cc *uint256.Int
	d  BlockNonce
	dd *BlockNonce
	e  common.Address
	ee *common.Address
	f  common.Hash
	ff *common.Hash
	g  Bloom
	gg *Bloom
	h  []byte
	hh *[]byte
	i  [][]byte
	ii []*[]byte
	j  []BlockNonce
	jj []*BlockNonce
	k  []common.Address
	kk []*common.Address
	l  []common.Hash
	ll []*common.Hash
	m  [10]byte
	mm *[245]byte
}
