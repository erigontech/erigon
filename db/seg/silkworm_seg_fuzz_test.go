// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

//go:build silkworm_seg_fuzz

package seg

import (
	"context"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeSegFilePath(path string, suffix string) string {
	return strings.TrimSuffix(path, filepath.Ext(path)) + suffix + ".seg"
}

func SegZipEx(ctx context.Context, words *RawWordsFile, outPath string, tmpDirPath string, logger log.Logger) error {
	compressor, err := NewCompressor(ctx, "SegZip", outPath, tmpDirPath, DefaultCfg, log.LvlDebug, logger)
	if err != nil {
		return err
	}
	defer compressor.Close()

	err = words.ForEach(func(word []byte, isCompressed bool) error {
		if isCompressed {
			return compressor.AddWord(word)
		} else {
			return compressor.AddUncompressedWord(word)
		}
	})
	if err != nil {
		return err
	}

	return compressor.Compress()
}

func SegZip(path string, tmpDirPath string) error {
	words, err := OpenRawWordsFile(path)
	if err != nil {
		return err
	}
	defer words.Close()
	return SegZipEx(context.Background(), words, makeSegFilePath(path, ""), tmpDirPath, log.New())
}

func SegUnzip(path string) error {
	decompressor, err := NewDecompressor(path)
	if err != nil {
		return err
	}
	defer decompressor.Close()

	outPath := strings.TrimSuffix(path, filepath.Ext(path)) + ".idt"
	words, err := NewRawWordsFile(outPath)
	if err != nil {
		return err
	}

	err = decompressor.WithReadAhead(func() error {
		word := make([]byte, 0)
		getter := decompressor.MakeGetter()
		for getter.HasNext() {
			word, _ = getter.Next(word[:0])
			appendErr := words.Append(word)
			if appendErr != nil {
				return appendErr
			}
		}
		return nil
	})
	if err != nil {
		words.CloseAndRemove()
		return err
	}

	words.Close()
	return nil
}

func SegZipSilkworm(path string, cmdPath string) error {
	cmd := exec.CommandContext(context.Background(), cmdPath, "seg_zip", path)
	return cmd.Run()
}

type RandPattern struct {
	pattern []byte
}

func NewRandPattern(r *rand.Rand, patternLen int) RandPattern {
	pattern := make([]byte, patternLen)
	r.Read(pattern)
	return RandPattern{pattern}
}

func (p RandPattern) CopyTo(word []byte, offset int) {
	copy(word[offset:min(offset+len(p.pattern), len(word))], p.pattern)
}

func generatePatterns(r *rand.Rand) []RandPattern {
	const minPatternLen = 64
	const maxPatternLen = 256
	const patternsCount = 16

	patterns := make([]RandPattern, 0, patternsCount)
	for i := 0; i < patternsCount; i++ {
		patternLen := r.Intn(maxPatternLen+1-minPatternLen) + minPatternLen
		pattern := NewRandPattern(r, patternLen)
		patterns = append(patterns, pattern)
	}
	return patterns
}

func generateRawWordsFile(path string, seed int64) (*RawWordsFile, error) {
	const maxTotalSize = int(512 * datasize.KB)
	const maxWordLen = int(1 * datasize.KB)
	const maxWordPatterns = 3

	words, err := NewRawWordsFile(path)
	if err != nil {
		return nil, err
	}

	r := rand.New(rand.NewSource(seed))
	totalSizeLimit := maxTotalSize // r.Intn(maxTotalSize + 1)
	var wordLen int

	patterns := generatePatterns(r)

	for totalSize := 0; totalSize < totalSizeLimit; totalSize += wordLen + 1 {
		// generate a random word
		wordLen = r.Intn(maxWordLen + 1)
		word := make([]byte, wordLen)
		r.Read(word)

		// fill it with some patterns at random offsets
		if wordLen > 0 {
			patternsCount := r.Intn(maxWordPatterns + 1)
			for i := 0; i < patternsCount; i++ {
				pattern := patterns[r.Intn(len(patterns))]
				offset := r.Intn(wordLen)
				pattern.CopyTo(word, offset)
			}
		}

		isCompressed := r.Intn(100) > 0
		if isCompressed {
			err = words.Append(word)
		} else {
			err = words.AppendUncompressed(word)
		}
		if err != nil {
			words.CloseAndRemove()
			return nil, err
		}
	}

	err = words.Flush()
	if err != nil {
		words.CloseAndRemove()
		return nil, err
	}

	return words, nil
}

func copyFiles(sourceFilePaths []string, targetDirPath string) {
	if len(targetDirPath) == 0 {
		return
	}
	for _, path := range sourceFilePaths {
		_ = exec.CommandContext(context.Background(), "cp", path, targetDirPath).Run()
	}
}

func FuzzSilkwormCompress(f *testing.F) {
	ctx := context.Background()
	logger := log.New()

	cmdPath, _ := os.LookupEnv("SILKWORM_SNAPSHOTS_CMD")
	require.NotEmpty(f, cmdPath, "Build silkworm snapshots command and set SILKWORM_SNAPSHOTS_CMD environment variable to the executable path, e.g. $HOME/silkworm/build/cmd/dev/snapshots")
	investigationDir, _ := os.LookupEnv("INVESTIGATION_DIR")

	f.Add(int64(0))
	f.Fuzz(func(t *testing.T, seed int64) {
		t.Helper()
		workDir := t.TempDir()

		path := filepath.Join(workDir, "words.idt")
		words, err := generateRawWordsFile(path, seed)
		require.NoError(t, err)

		// compress using erigon
		outPath := makeSegFilePath(path, "_erigon")
		err = SegZipEx(ctx, words, outPath, t.TempDir(), logger)
		words.Close()
		require.NoError(t, err)

		// compress using silkworm
		outPathSilkworm := makeSegFilePath(path, "")
		err = SegZipSilkworm(path, cmdPath)
		if err != nil {
			copyFiles([]string{path, outPath}, investigationDir)
		}
		require.NoError(t, err)

		outPathCRC := checksum(outPath)
		outPathSilkwormCRC := checksum(outPathSilkworm)
		if outPathCRC != outPathSilkwormCRC {
			assert.Equal(t, outPathCRC, outPathSilkwormCRC)
			copyFiles([]string{path, outPath}, investigationDir)
		}
	})
}
