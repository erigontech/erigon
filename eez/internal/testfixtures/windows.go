// Copyright 2026 The Erigon Authors
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

package testfixtures

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

const windowsDir = "windows"

// WindowNames lists the vendored windows.
var WindowNames = []string{
	"stateless-block-13",
	"stateless-checkpoint-2175",
	"nonzero-outbound-630",
	"captured-devnet-window-84",
}

// Window is a run of consecutive recorded L2 blocks with their execution
// witnesses, captured by the reference client.
type Window struct {
	Name   string
	Blocks []WindowBlock
	// ChainConfig is the raw chain configuration the window executes under.
	ChainConfig []byte
	// Oracle holds the recorded expected values; keys differ per window.
	Oracle map[string]json.RawMessage
	// PostBatch is the mined postAndVerifyBatch calldata, when recorded as hex.
	PostBatch []byte
}

// WindowBlock is one recorded block.
type WindowBlock struct {
	Number  uint64
	RLP     []byte
	Witness *Witness
	// Hash and ParentHash come from blocks.json and are zero without one.
	Hash       common.Hash
	ParentHash common.Hash
}

// Witness is an execution witness in the shape of reth's debug_executionWitness.
type Witness struct {
	State   []hexutil.Bytes `json:"state"`
	Codes   []hexutil.Bytes `json:"codes"`
	Keys    []hexutil.Bytes `json:"keys"`
	Headers []hexutil.Bytes `json:"headers"`
}

var (
	blockFileRe   = regexp.MustCompile(`^(?:checkpoint-)?block-(\d+)\.rlp(\.hex)?$`)
	witnessFileRe = regexp.MustCompile(`^(?:checkpoint-)?witness-(\d+)\.json$`)
	configFileRe  = regexp.MustCompile(`^(?:checkpoint-)?chain-config\.json$`)
	oracleFileRe  = regexp.MustCompile(`^(?:checkpoint-)?oracle(?:-\d+)?\.json$`)
)

// LoadWindow loads windows/<name>.
func LoadWindow(name string) (*Window, error) {
	dir := Path(windowsDir, name)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	w := &Window{Name: name}
	blocks := map[uint64]*WindowBlock{}
	block := func(n uint64) *WindowBlock {
		if blocks[n] == nil {
			blocks[n] = &WindowBlock{Number: n}
		}
		return blocks[n]
	}
	for _, f := range files {
		fname := f.Name()
		path := filepath.Join(dir, fname)
		switch {
		case blockFileRe.MatchString(fname):
			m := blockFileRe.FindStringSubmatch(fname)
			n, err := strconv.ParseUint(m[1], 10, 64)
			if err != nil {
				return nil, err
			}
			if block(n).RLP, err = readBlob(path, m[2] != ""); err != nil {
				return nil, err
			}
		case witnessFileRe.MatchString(fname):
			n, err := strconv.ParseUint(witnessFileRe.FindStringSubmatch(fname)[1], 10, 64)
			if err != nil {
				return nil, err
			}
			var wit Witness
			if err := readStrict(path, &wit); err != nil {
				return nil, err
			}
			block(n).Witness = &wit
		case configFileRe.MatchString(fname):
			if w.ChainConfig, err = os.ReadFile(path); err != nil {
				return nil, err
			}
		case oracleFileRe.MatchString(fname):
			if err := readStrict(path, &w.Oracle); err != nil {
				return nil, err
			}
		case fname == "postbatch.hex":
			if w.PostBatch, err = readBlob(path, true); err != nil {
				return nil, err
			}
		case fname == "blocks.json":
			var entries []struct {
				Hash       common.Hash `json:"hash"`
				Number     uint64      `json:"number"`
				ParentHash common.Hash `json:"parent_hash"`
			}
			if err := readStrict(path, &entries); err != nil {
				return nil, err
			}
			for _, e := range entries {
				b := block(e.Number)
				b.Hash, b.ParentHash = e.Hash, e.ParentHash
			}
		}
	}
	for _, n := range slices.Sorted(maps.Keys(blocks)) {
		b := blocks[n]
		if b.RLP == nil || b.Witness == nil {
			return nil, fmt.Errorf("window %s: block %d lacks its RLP or witness", name, n)
		}
		if len(w.Blocks) > 0 && w.Blocks[len(w.Blocks)-1].Number+1 != n {
			return nil, fmt.Errorf("window %s: block %d does not follow block %d", name, n, w.Blocks[len(w.Blocks)-1].Number)
		}
		w.Blocks = append(w.Blocks, *b)
	}
	if len(w.Blocks) == 0 || w.ChainConfig == nil {
		return nil, fmt.Errorf("window %s: no blocks or no chain config", name)
	}
	return w, nil
}

// OracleHash returns the 32-byte oracle value stored under key.
func (w *Window) OracleHash(key string) (common.Hash, error) {
	raw, ok := w.Oracle[key]
	if !ok {
		return common.Hash{}, fmt.Errorf("window %s: oracle has no %q", w.Name, key)
	}
	var h common.Hash
	if err := json.Unmarshal(raw, &h); err != nil {
		return common.Hash{}, fmt.Errorf("window %s: oracle %q: %w", w.Name, key, err)
	}
	return h, nil
}

// readBlob reads a file holding raw bytes, or hex with an optional 0x prefix.
func readBlob(path string, isHex bool) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil || !isHex {
		return data, err
	}
	s := strings.TrimPrefix(strings.TrimSpace(string(data)), "0x")
	out, err := hexutil.Decode("0x" + s)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return out, nil
}

func readStrict(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := decodeStrict(data, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}
