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

package v4

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errTrieReleased = errors.New("commitment v4: trie released")
	errTrieContext  = errors.New("commitment v4: missing Patricia context")
)

type Trie struct {
	ctx    commitment.PatriciaContext
	root   []byte
	traceW io.Writer
}

func NewTrie(tmpdir string, cfg commitment.TrieConfig) (commitment.Trie, *commitment.Updates) {
	return &Trie{}, commitment.NewUpdates(commitment.ModeUpdate, tmpdir, commitment.KeyToHexNibbleHash)
}

func init() {
	commitment.RegisterTrieFunc(commitment.VariantCommitmentV4, NewTrie)
}

func (t *Trie) RootHash() ([]byte, error) {
	if t == nil {
		return nil, errTrieReleased
	}
	if len(t.root) == 0 {
		return bytes.Clone(empty.RootHash[:]), nil
	}
	return bytes.Clone(t.root), nil
}

func (t *Trie) SetTraceWriter(w io.Writer) {
	if t != nil {
		t.traceW = w
	}
}

func (t *Trie) Variant() commitment.TrieVariant {
	return commitment.VariantCommitmentV4
}

func (t *Trie) Reset() {
	if t != nil {
		t.root = nil
	}
}

func (t *Trie) ResetContext(ctx commitment.PatriciaContext) {
	if t != nil {
		t.ctx = ctx
	}
}

func (t *Trie) Process(
	ctx context.Context,
	updates *commitment.Updates,
	logPrefix string,
	onProgress func(*commitment.CommitProgress),
	warmup commitment.WarmupConfig,
) ([]byte, error) {
	if t == nil {
		return nil, errTrieReleased
	}
	if t.ctx == nil {
		return nil, errTrieContext
	}
	if updates == nil {
		return nil, errors.New("commitment v4: nil updates")
	}
	if updates.Mode() != commitment.ModeUpdate {
		return nil, errors.New("commitment v4: Process requires ModeUpdate updates")
	}

	var stream []phaseAInput
	err := updates.HashSort(ctx, nil, func(hashedKey, plainKey []byte, update *commitment.Update) error {
		stream = append(stream, phaseAInput{
			hashedKey: bytes.Clone(hashedKey),
			plainKey:  bytes.Clone(plainKey),
			update:    cloneUpdate(update),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	storage, accounts := partition(stream)
	roots := make(map[[32]byte][32]byte, len(storage))
	for _, task := range storage {
		root, err := runStorageTask(t.ctx, task)
		if err != nil {
			return nil, err
		}
		roots[task.addrHash] = root
	}
	root, err := runAccountTrie(t.ctx, accounts, roots)
	if err != nil {
		return nil, err
	}
	t.root = append(t.root[:0], root[:]...)
	if onProgress != nil {
		onProgress(&commitment.CommitProgress{KeyIndex: uint64(len(stream)), UpdateCount: uint64(len(stream))})
	}
	return bytes.Clone(t.root), nil
}

func (t *Trie) Release() {
	if t != nil {
		t.ctx = nil
		t.root = nil
		t.traceW = nil
	}
}
