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
package recording

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/erigontech/erigon/common"
	commondir "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/types"
)

// Files of a recording directory.
const (
	HeadersFile = "l1-headers.json"
	LogsFile    = "l1-logs.json"
	TxsFile     = "l1-txs.json"
	// StateFile holds the answers to state reads; a recording without any
	// has none.
	StateFile = "l1-state.json"
)

type recordedHeaders struct {
	ChainID   hexutil.Uint64   `json:"chainId"`
	Finalized *hexutil.Uint64  `json:"finalized,omitempty"`
	Canonical []recordedHeader `json:"canonical"`
	Orphans   []recordedHeader `json:"orphans,omitempty"`
}

type recordedHeader struct {
	Number     hexutil.Uint64 `json:"number"`
	Hash       common.Hash    `json:"hash"`
	ParentHash common.Hash    `json:"parentHash"`
	Timestamp  hexutil.Uint64 `json:"timestamp"`
	MixHash    common.Hash    `json:"mixHash,omitzero"`
}

type recordedTx struct {
	BlockHash common.Hash    `json:"blockHash"`
	Index     hexutil.Uint64 `json:"index"`
	Raw       hexutil.Bytes  `json:"raw"`
}

type recordedState struct {
	BlockHash common.Hash    `json:"blockHash"`
	Code      []recordedCode `json:"code,omitempty"`
	Calls     []recordedCall `json:"calls,omitempty"`
}

type recordedCode struct {
	Address common.Address `json:"address"`
	Code    hexutil.Bytes  `json:"code"`
}

type recordedCall struct {
	To       common.Address `json:"to"`
	Data     hexutil.Bytes  `json:"data"`
	Return   hexutil.Bytes  `json:"return"`
	Reverted bool           `json:"reverted,omitempty"`
}

func recordState(b *Block) *recordedState {
	if len(b.Code) == 0 && len(b.Calls) == 0 {
		return nil
	}
	s := &recordedState{BlockHash: b.Header.Hash}
	for _, addr := range slices.SortedFunc(maps.Keys(b.Code), common.Address.Cmp) {
		s.Code = append(s.Code, recordedCode{Address: addr, Code: append(hexutil.Bytes{}, b.Code[addr]...)})
	}
	for _, c := range b.Calls {
		s.Calls = append(s.Calls, recordedCall{To: c.Call.To, Data: c.Call.Data, Return: append(hexutil.Bytes{}, c.Return...), Reverted: c.Reverted})
	}
	return s
}

func (s *recordedState) apply(b *Block) error {
	if b.Code != nil || b.Calls != nil {
		return fmt.Errorf("%s: state of block %x recorded twice", StateFile, s.BlockHash)
	}
	b.Code = make(map[common.Address][]byte, len(s.Code))
	for _, c := range s.Code {
		if _, ok := b.Code[c.Address]; ok {
			return fmt.Errorf("%s: code of %x at block %x recorded twice", StateFile, c.Address, s.BlockHash)
		}
		b.Code[c.Address] = c.Code
	}
	for _, c := range s.Calls {
		b.Calls = append(b.Calls, CallResult{Call: l1.Call{To: c.To, Data: c.Data}, Return: c.Return, Reverted: c.Reverted})
	}
	return nil
}

func (h recordedHeader) header() l1.Header {
	return l1.Header{Number: uint64(h.Number), Hash: h.Hash, ParentHash: h.ParentHash, Time: uint64(h.Timestamp), MixDigest: h.MixHash}
}

func recordHeader(h l1.Header) recordedHeader {
	return recordedHeader{Number: hexutil.Uint64(h.Number), Hash: h.Hash, ParentHash: h.ParentHash, Timestamp: hexutil.Uint64(h.Time), MixHash: h.MixDigest}
}

// Load returns the chain recorded in dir. The canonical headers
// must chain, every log and state answer must sit in a recorded block, and a
// transaction must hash to what the logs of its position name.
func Load(dir string) (*MemoryChain, error) {
	var hs recordedHeaders
	if err := readRecording(dir, HeadersFile, &hs); err != nil {
		return nil, err
	}
	var logs []*types.Log
	if err := readRecording(dir, LogsFile, &logs); err != nil {
		return nil, err
	}
	var txs []recordedTx
	if err := readRecording(dir, TxsFile, &txs); err != nil {
		return nil, err
	}
	var state []recordedState
	if err := readRecording(dir, StateFile, &state); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if len(hs.Canonical) == 0 {
		return nil, fmt.Errorf("%s: no canonical headers", HeadersFile)
	}

	blocks := map[common.Hash]*Block{}
	for _, h := range slices.Concat(hs.Canonical, hs.Orphans) {
		if blocks[h.Hash] != nil {
			return nil, fmt.Errorf("%s: block %x recorded twice", HeadersFile, h.Hash)
		}
		blocks[h.Hash] = &Block{Header: h.header(), Recorded: true}
	}
	for _, l := range logs {
		b := blocks[l.BlockHash]
		if b == nil || b.Header.Number != uint64(l.BlockNumber) {
			return nil, fmt.Errorf("%s: log %d of block %d (%x) outside the recorded blocks", LogsFile, l.Index, l.BlockNumber, l.BlockHash)
		}
		b.Logs = append(b.Logs, l)
	}
	for _, rt := range txs {
		b := blocks[rt.BlockHash]
		if b == nil {
			return nil, fmt.Errorf("%s: transaction of unrecorded block %x", TxsFile, rt.BlockHash)
		}
		tx, err := types.UnmarshalTransactionFromBinary(rt.Raw, false)
		if err != nil {
			return nil, fmt.Errorf("%s: transaction %d of block %x: %w", TxsFile, rt.Index, rt.BlockHash, err)
		}
		for _, l := range b.Logs {
			if uint64(l.TxIndex) == uint64(rt.Index) && l.TxHash != tx.Hash() {
				return nil, fmt.Errorf("%w: %s: transaction %d of block %x hashes to %x, its logs name %x",
					l1.ErrInconsistent, TxsFile, rt.Index, rt.BlockHash, tx.Hash(), l.TxHash)
			}
		}
		for uint64(len(b.Txs)) <= uint64(rt.Index) {
			b.Txs = append(b.Txs, nil)
		}
		b.Txs[rt.Index] = tx
	}
	for _, s := range state {
		b := blocks[s.BlockHash]
		if b == nil {
			return nil, fmt.Errorf("%s: state of unrecorded block %x", StateFile, s.BlockHash)
		}
		if err := s.apply(b); err != nil {
			return nil, err
		}
	}

	c, err := NewMemoryChain(uint64(hs.ChainID), blocks[hs.Canonical[0].Hash])
	if err != nil {
		return nil, err
	}
	for _, h := range hs.Canonical[1:] {
		if err := c.Append(blocks[h.Hash]); err != nil {
			return nil, fmt.Errorf("%s: %w", HeadersFile, err)
		}
	}
	c.mu.Lock()
	for _, h := range hs.Orphans {
		c.store(blocks[h.Hash])
	}
	c.mu.Unlock()
	if hs.Finalized != nil {
		if err := c.Finalize(uint64(*hs.Finalized)); err != nil {
			return nil, fmt.Errorf("%s: %w", HeadersFile, err)
		}
	}
	return c, nil
}

// Write writes c to dir in the layout Load reads.
func Write(dir string, c *MemoryChain) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	hs := recordedHeaders{ChainID: hexutil.Uint64(c.chainID)}
	if c.finalized != nil {
		f := hexutil.Uint64(*c.finalized)
		hs.Finalized = &f
	}
	var logs []*types.Log
	var txs []recordedTx
	var state []*recordedState
	add := func(b *Block) error {
		logs = append(logs, b.Logs...)
		if s := recordState(b); s != nil {
			state = append(state, s)
		}
		for i, tx := range b.Txs {
			if tx == nil {
				continue
			}
			var raw bytes.Buffer
			if err := tx.MarshalBinary(&raw); err != nil {
				return err
			}
			txs = append(txs, recordedTx{BlockHash: b.Header.Hash, Index: hexutil.Uint64(i), Raw: raw.Bytes()})
		}
		return nil
	}
	canonical := map[common.Hash]bool{}
	for _, h := range c.canonical {
		canonical[h] = true
		hs.Canonical = append(hs.Canonical, recordHeader(c.blocks[h].Header))
		if err := add(c.blocks[h]); err != nil {
			return err
		}
	}
	orphans := make([]*Block, 0, len(c.blocks)-len(c.canonical))
	for h, b := range c.blocks {
		if !canonical[h] {
			orphans = append(orphans, b)
		}
	}
	slices.SortFunc(orphans, func(a, b *Block) int { return bytes.Compare(a.Header.Hash[:], b.Header.Hash[:]) })
	for _, b := range orphans {
		hs.Orphans = append(hs.Orphans, recordHeader(b.Header))
		if err := add(b); err != nil {
			return err
		}
	}
	files := map[string]any{HeadersFile: hs, LogsFile: logs, TxsFile: txs}
	if len(state) > 0 {
		files[StateFile] = state
	} else if err := commondir.RemoveFile(filepath.Join(dir, StateFile)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for name, v := range files {
		data, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func readRecording(dir, name string, v any) error {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}
