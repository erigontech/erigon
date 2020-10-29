package stateless

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

type statsColumn struct {
	name   string
	getter func(*trie.BlockWitnessStats) uint64
}

var columns = []statsColumn{
	{"BlockWitnessSize", func(s *trie.BlockWitnessStats) uint64 { return s.BlockWitnessSize() }},
	{"CodesSize", func(s *trie.BlockWitnessStats) uint64 { return s.CodesSize() }},
	{"LeafKeysSize", func(s *trie.BlockWitnessStats) uint64 { return s.LeafKeysSize() }},
	{"LeafValuesSize", func(s *trie.BlockWitnessStats) uint64 { return s.LeafValuesSize() }},
	{"StructureSize", func(s *trie.BlockWitnessStats) uint64 { return s.StructureSize() }},
	{"HashesSize", func(s *trie.BlockWitnessStats) uint64 { return s.HashesSize() }},
}

type StatsFile struct {
	file      io.WriteCloser
	buffer    *csv.Writer
	hasHeader bool
}

func NewStatsFile(path string) (*StatsFile, error) {
	_, err := os.Stat(path)
	appending := err == nil || !os.IsNotExist(err)

	w, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &StatsFile{file: w, buffer: csv.NewWriter(w), hasHeader: appending}, nil
}

func (s *StatsFile) writeHeader() error {
	header := make([]string, len(columns)+1)

	header[0] = "BlockNumber"
	for i, col := range columns {
		header[i+1] = col.name
	}

	return s.buffer.Write(header)
}

func (s *StatsFile) AddRow(blockNumber uint64, row *trie.BlockWitnessStats) error {
	if !s.hasHeader {
		fmt.Println("writing header")
		if err := s.writeHeader(); err != nil {
			return err
		}
		s.hasHeader = true
	}

	fields := make([]string, len(columns)+1)

	fields[0] = stringify(blockNumber)
	for i, col := range columns {
		fields[i+1] = stringify(col.getter(row))
	}

	return s.buffer.Write(fields)
}

func (s *StatsFile) Close() error {
	s.buffer.Flush()
	if err := s.buffer.Error(); err != nil {
		return err
	}
	return s.file.Close()
}

func stringify(v uint64) string {
	return fmt.Sprintf("%d", v)
}
