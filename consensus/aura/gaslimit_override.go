package aura

import (
	lru "github.com/hashicorp/golang-lru/v2"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
)

type GasLimitOverride struct {
	cache *lru.Cache[libcommon.Hash, *uint256.Int]
}

func NewGasLimitOverride() *GasLimitOverride {
	// The number of recent block hashes for which the gas limit override is memoized.
	const GasLimitOverrideCacheCapacity = 10

	cache, err := lru.New[libcommon.Hash, *uint256.Int](GasLimitOverrideCacheCapacity)
	if err != nil {
		panic("error creating prefetching cache for blocks")
	}
	return &GasLimitOverride{cache: cache}
}

func (pb *GasLimitOverride) Pop(hash libcommon.Hash) *uint256.Int {
	if val, ok := pb.cache.Get(hash); ok && val != nil {
		pb.cache.Remove(hash)
		return val
	}
	return nil
}

func (pb *GasLimitOverride) Add(hash libcommon.Hash, b *uint256.Int) {
	if b == nil {
		return
	}
	pb.cache.ContainsOrAdd(hash, b)
}

func (c *AuRa) HasGasLimitContract() bool {
	return len(c.cfg.BlockGasLimitContractTransitions) != 0
}

func (c *AuRa) GetBlockGasLimitFromContract(_ *chain.Config, syscall consensus.SystemCall) uint64 {
	// var blockLimitContract
	addr, ok := c.cfg.BlockGasLimitContractTransitions[0]
	if !ok {
		return 0
	}
	gasLimit := callBlockGasLimitAbi(addr, syscall)
	return gasLimit.Uint64()
}

func (c *AuRa) verifyGasLimitOverride(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState, syscallCustom consensus.SysCallCustom) {
	//IsPoSHeader check is necessary as merge.go calls Initialize on AuRa indiscriminately
	gasLimitOverride := c.HasGasLimitContract() && !misc.IsPoSHeader(header)
	if gasLimitOverride {
		syscallPrevHeader := func(addr libcommon.Address, data []byte) ([]byte, error) {
			return syscallCustom(addr, data, state, chain.GetHeaderByHash(header.ParentHash), true)
		}
		blockGasLimit := c.GetBlockGasLimitFromContract(config, syscallPrevHeader)

		if blockGasLimit > 0 {
			if header.GasLimit != blockGasLimit {
				panic("Block gas limit doesn't match BlockGasLimitContract with AuRa")
			}
		}
	}
}
