package trie

import "io"

type StatsColumn string

const (
	ColumnStructure  = StatsColumn("structure")
	ColumnHashes     = StatsColumn("hashes")
	ColumnCodes      = StatsColumn("codes")
	ColumnLeafKeys   = StatsColumn("leaf_keys")
	ColumnLeafValues = StatsColumn("leaf_values")
	ColumnTotal      = StatsColumn("total_witness_size")
)

type WitnessStatsCollector struct {
	currentColumn StatsColumn
	w             io.Writer
	stats         map[StatsColumn]uint64
	total         uint64
}

func NewWitnessStatsCollector(w io.Writer) *WitnessStatsCollector {
	return &WitnessStatsCollector{w: w, stats: make(map[StatsColumn]uint64)}
}

func (w *WitnessStatsCollector) Write(p []byte) (int, error) {
	val := w.stats[w.currentColumn]

	written, err := w.w.Write(p)

	val += uint64(written)
	w.total += uint64(written)

	w.stats[w.currentColumn] = val

	return written, err
}

func (w *WitnessStatsCollector) WithColumn(column StatsColumn) io.Writer {
	w.currentColumn = column
	return w
}

func (w *WitnessStatsCollector) GetStats() *BlockWitnessStats {
	return &BlockWitnessStats{
		witnessSize: w.total,
		stats:       w.stats,
	}
}

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
