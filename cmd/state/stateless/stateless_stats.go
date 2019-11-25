package stateless

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/ledgerwatch/turbo-geth/trie"
)

var keys = []string{"balances", "codes", "hashes", "keys", "nonces", "structure", "values"}

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
	return s.buffer.Write(
		append([]string{"block-number", "witness"}, keys...),
	)
}

func (s *StatsFile) AddRow(blockNum uint64, witness []byte, tapeStats trie.WitnessTapeStats) error {
	if !s.hasHeader {
		fmt.Println("writing header")
		if err := s.writeHeader(); err != nil {
			return err
		}
		s.hasHeader = true
	}
	fields := make([]string, 2+len(keys))
	fieldIndex := 0

	fields[fieldIndex] = stringify(blockNum)
	fieldIndex++

	fields[fieldIndex] = stringify(uint64(len(witness)))
	fieldIndex++

	for _, key := range keys {
		fields[fieldIndex] = stringify(uint64(tapeStats.GetOrZero(key)))
		fieldIndex++
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
