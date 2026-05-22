package trie

type StatsColumn string

const (
	ColumnStructure  = StatsColumn("structure")
	ColumnHashes     = StatsColumn("hashes")
	ColumnCodes      = StatsColumn("codes")
	ColumnLeafKeys   = StatsColumn("leaf_keys")
	ColumnLeafValues = StatsColumn("leaf_values")
	ColumnTotal      = StatsColumn("total_witness_size")
)

type BlockWitnessStats struct {
	witnessSize uint64
	stats       map[StatsColumn]uint64
}

func (s *BlockWitnessStats) BlockWitnessSize() uint64 {
	return s.witnessSize
}

func (s *BlockWitnessStats) CodesSize() uint64 {
	return s.stats[ColumnCodes]
}

func (s *BlockWitnessStats) LeafKeysSize() uint64 {
	return s.stats[ColumnLeafKeys]
}

func (s *BlockWitnessStats) LeafValuesSize() uint64 {
	return s.stats[ColumnLeafValues]
}

func (s *BlockWitnessStats) StructureSize() uint64 {
	return s.stats[ColumnStructure]
}

func (s *BlockWitnessStats) HashesSize() uint64 {
	return s.stats[ColumnHashes]
}
