package state

import "github.com/ledgerwatch/turbo-geth/trie"

type BlockWitnessStats struct {
	blockNr     uint64
	witnessSize uint64
	tapeStats   trie.WitnessTapeStats
}

func (s *BlockWitnessStats) tapeSize(tapeKey string) uint64 {
	return uint64(s.tapeStats[tapeKey])
}

func (s *BlockWitnessStats) BlockNumber() uint64 {
	return s.blockNr
}

func (s *BlockWitnessStats) BlockWitnessSize() uint64 {
	return s.witnessSize
}

func (s *BlockWitnessStats) CodesSize() uint64 {
	return s.tapeSize("codes")
}

func (s *BlockWitnessStats) LeafKeysSize() uint64 {
	return s.tapeSize("keys")
}

func (s *BlockWitnessStats) LeafValuesSize() uint64 {
	return s.tapeSize("values")
}

func (s *BlockWitnessStats) MasksSize() uint64 {
	return s.tapeSize("structure")
}

func (s *BlockWitnessStats) HashesSize() uint64 {
	return s.tapeSize("hashes")
}

/*
	TODO: Implement the same values for contracts too as soon as the info is available

	func (s *BlockWitnessStats) ContractsSize() uint64 {
		panic("implement me")
	}

	func (s *BlockWitnessStats) WitnessSizeNoContracts() uint64 {
		return s.BlockWitnessSize() - s.ContractsSize()
	}

	func (s *BlockWitnessStats) ContractLeafKeysSize() uint64 {
		panic("implement me")
	}

	func (s *BlockWitnessStats) ContactLeafValuesSize() uint64 {
		panic("implement me")
	}

	func (s *BlockWitnessStats) ContractMasks() uint64 {
		panic("implement me")
	}

	func (s *BlockWitnessStats) ContractHashes() uint64 {
		panic("implement me")
	}
*/

func NewBlockWitnessStats(blockNr uint64, witnessSize uint64, tapeStats trie.WitnessTapeStats) *BlockWitnessStats {
	return &BlockWitnessStats{
		blockNr:     blockNr,
		witnessSize: witnessSize,
		tapeStats:   tapeStats,
	}
}
