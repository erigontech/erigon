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

package commitment

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

type pbinUpdateSink func(treeKey, plainKey []byte, update *Update) error

type pbinUpdateStream struct {
	state PatriciaContext
	emit  pbinUpdateSink

	siblingKey [pbinAccountKeyLength]byte
	codeChunks []pbinCodeChunk
	keyDigest  pbinDigestCache

	// witness is what the parent state cannot tell a witness pass about the block.
	// See chunkSource and removesAccount.
	witness     PBinWitnessBlock
	witnessPass bool

	// pendingRemoval holds storage-subtree prefixes waiting for the walk to reach
	// their zone. Removals are queued in account order, which is prefix order.
	pendingRemoval [][]byte
}

type pbinCodeChunk struct {
	key   [pbinCodeKeyLength]byte
	value [pbinValueLength]byte
}

type pbinCodeContext interface {
	Code(plainKey []byte) ([]byte, error)
}

func (s *pbinUpdateStream) process(ctx context.Context, updates *Updates, state PatriciaContext, emit pbinUpdateSink) (uint64, error) {
	s.reset()
	s.state, s.emit = state, emit
	defer func() { s.state, s.emit = nil, nil }()

	var processed uint64
	err := updates.HashSort(ctx, nil, func(treeKey, plainKey []byte, stateUpdate *Update) error {
		if err := s.processKey(treeKey, plainKey, stateUpdate); err != nil {
			return err
		}
		processed++
		return nil
	})
	if err != nil {
		return processed, err
	}
	if err = s.flushCodeChunks(); err != nil {
		return processed, err
	}
	if err = s.flushRemovals(nil); err != nil {
		return processed, err
	}
	return processed, nil
}

func (s *pbinUpdateStream) reset() {
	s.state, s.emit = nil, nil
	s.codeChunks = s.codeChunks[:0]
	s.pendingRemoval = s.pendingRemoval[:0]
}

func (s *pbinUpdateStream) release() {
	s.reset()
	s.keyDigest = pbinDigestCache{}
	s.witness = PBinWitnessBlock{}
}

// processKey expands an account into its header leaves. Code chunks are delayed
// until emitting them cannot move the ordered trie walk back.
func (s *pbinUpdateStream) processKey(treeKey, plainKey []byte, stateUpdate *Update) error {
	if err := s.flushCodeChunksBefore(treeKey); err != nil {
		return err
	}
	if err := s.flushRemovalsBefore(treeKey); err != nil {
		return err
	}
	update := stateUpdate
	if update == nil {
		var err error
		if update, err = s.stateOf(plainKey); err != nil {
			return err
		}
	}
	if len(plainKey) == length.Addr && s.removesAccount(plainKey, update) {
		if err := s.removeAccount(plainKey); err != nil {
			return err
		}
	}
	if err := s.emit(treeKey, plainKey, update); err != nil {
		return err
	}
	if len(plainKey) != length.Addr {
		return nil
	}
	return s.emitCodeLeaves(treeKey, plainKey, update)
}

// emitCodeLeaves writes the header sibling the account's code selects —
// CODE_HASH, or DELEGATION for an EIP-7702 indicator — and removes the other.
// The stream is told nothing about what the account held before, so both
// removals are unconditional. The indicator is no account field, so its leaf
// carries the value itself and no plain key.
func (s *pbinUpdateStream) emitCodeLeaves(basicDataKey, plainKey []byte, update *Update) error {
	code, codeHash, err := s.chunkSource(plainKey, update)
	if err != nil {
		return err
	}
	if pbinIsDelegation(code) {
		if err := s.emitSibling(basicDataKey, pbinCodeHashLeafKey, plainKey, &Update{Flags: DeleteUpdate}); err != nil {
			return err
		}
		indicator := Update{Flags: StorageUpdate, StorageLen: pbinValueLength, Storage: pbinEncodeDelegation(code)}
		return s.emitSibling(basicDataKey, pbinDelegationLeafKey, nil, &indicator)
	}
	if err := s.emitSibling(basicDataKey, pbinCodeHashLeafKey, plainKey, update); err != nil {
		return err
	}
	if err := s.emitSibling(basicDataKey, pbinDelegationLeafKey, plainKey, &Update{Flags: DeleteUpdate}); err != nil {
		return err
	}
	s.queueChunks(code, codeHash)
	return nil
}

// removesAccount reports whether the block removes this account. A witness pass
// reads the parent state, where an account the block creates is absent too, so
// there it has to be told rather than infer it.
func (s *pbinUpdateStream) removesAccount(plainKey []byte, update *Update) bool {
	if s.witnessPass {
		_, removed := s.witness.Removed[string(plainKey)]
		return removed
	}
	return update.Deleted()
}

// removeAccount drops the two subtrees an account owns — its header stem, and
// its storage prefix once the walk reaches that zone — rather than the leaves it
// holds, which for storage nothing enumerates. Code chunks always stay, which
// parts from the reference suite when the removed account was the sole holder;
// EIP-6780 bounds that to states the chain cannot reach, since an account
// deleted with its code was created in the same transaction and a
// create-and-destroy merges to a bare deletion that inserts no chunk
// (eip:"Zero values and deletion").
func (s *pbinUpdateStream) removeAccount(plainKey []byte) error {
	drop := Update{Flags: DeleteUpdate}
	if err := s.emit(s.keyDigest.accountHeaderStem(plainKey), plainKey, &drop); err != nil {
		return err
	}
	s.pendingRemoval = append(s.pendingRemoval, s.keyDigest.accountStoragePrefix(plainKey))
	return nil
}

func (s *pbinUpdateStream) flushRemovalsBefore(treeKey []byte) error {
	if len(s.pendingRemoval) == 0 || treeKey[0] < pbinStorageZone {
		return nil
	}
	return s.flushRemovals(treeKey)
}

// flushRemovals emits the queued storage-prefix drops that sort before upTo, or
// all of them when upTo is nil. A drop has to land before any storage key it
// covers; a queue out of order would fail the engine's ascending-visit check
// rather than pass silently.
func (s *pbinUpdateStream) flushRemovals(upTo []byte) error {
	drop := Update{Flags: DeleteUpdate}
	sent := 0
	for _, prefix := range s.pendingRemoval {
		if upTo != nil && bytes.Compare(prefix, upTo) >= 0 {
			break
		}
		if err := s.emit(prefix, nil, &drop); err != nil {
			return err
		}
		sent++
	}
	s.pendingRemoval = append(s.pendingRemoval[:0], s.pendingRemoval[sent:]...)
	return nil
}

// chunkSource is the code an account's chunk keys derive from, with the hash
// addressing its chunks. A witness pass walks the parent state, where a
// contract the block creates has no code, so it needs the override to reach the
// same keys the fold did. Only key derivation moves; values stay pre-state.
// A deletion's code fields are whatever the batch merge left behind, not
// state, so a removed account is codeless here.
func (s *pbinUpdateStream) chunkSource(plainKey []byte, update *Update) ([]byte, common.Hash, error) {
	if code, ok := s.witness.Code[string(plainKey)]; ok && s.witnessPass {
		return code, common.Hash(keccak.Sum256(code)), nil
	}
	if update.Deleted() || update.CodeSize == 0 {
		return nil, common.Hash{}, nil
	}
	code, err := s.codeOf(plainKey)
	if err != nil {
		return nil, common.Hash{}, err
	}
	if uint64(len(code)) != update.CodeSize {
		return nil, common.Hash{}, fmt.Errorf("pbin: account %x says %d code bytes, the code domain holds %d",
			plainKey, update.CodeSize, len(code))
	}
	return code, update.CodeHash, nil
}

func (s *pbinUpdateStream) queueChunks(code []byte, codeHash common.Hash) {
	for i, chunk := range pbinChunkifyCode(code) {
		var cc pbinCodeChunk
		copy(cc.key[:], s.keyDigest.codeChunkKey(codeHash, i))
		cc.value = chunk
		s.codeChunks = append(s.codeChunks, cc)
	}
}

func (s *pbinUpdateStream) flushCodeChunksBefore(treeKey []byte) error {
	if len(s.codeChunks) == 0 || treeKey[0] <= pbinCodeZone {
		return nil
	}
	return s.flushCodeChunks()
}

func (s *pbinUpdateStream) flushCodeChunks() error {
	if len(s.codeChunks) == 0 {
		return nil
	}
	slices.SortFunc(s.codeChunks, func(a, b pbinCodeChunk) int { return bytes.Compare(a.key[:], b.key[:]) })

	var prev *pbinCodeChunk
	for i := range s.codeChunks {
		cc := &s.codeChunks[i]
		if prev != nil && cc.key == prev.key {
			if cc.value != prev.value {
				return fmt.Errorf("pbin: code chunk %x carries two values", cc.key[:])
			}
			continue
		}
		update := Update{Flags: StorageUpdate, StorageLen: pbinValueLength, Storage: cc.value}
		if err := s.emit(cc.key[:], nil, &update); err != nil {
			return err
		}
		prev = cc
	}
	s.codeChunks = s.codeChunks[:0]
	return nil
}

func (s *pbinUpdateStream) codeOf(plainKey []byte) ([]byte, error) {
	ctx, ok := s.state.(pbinCodeContext)
	if !ok {
		return nil, fmt.Errorf("%w: %T serves no code, needed to chunk account %x",
			ErrPBinUnsupported, s.state, plainKey)
	}
	code, err := ctx.Code(plainKey)
	if err != nil {
		return nil, fmt.Errorf("pbin: read code %x: %w", plainKey, err)
	}
	return code, nil
}

func (s *pbinUpdateStream) stateOf(plainKey []byte) (*Update, error) {
	if len(plainKey) == length.Addr {
		update, err := s.state.Account(plainKey)
		if err != nil {
			return nil, fmt.Errorf("pbin: read account %x: %w", plainKey, err)
		}
		return update, nil
	}
	update, err := s.state.Storage(plainKey)
	if err != nil {
		return nil, fmt.Errorf("pbin: read storage %x: %w", plainKey, err)
	}
	return update, nil
}

func (s *pbinUpdateStream) emitSibling(basicDataKey []byte, subIndex byte, plainKey []byte, update *Update) error {
	if len(basicDataKey) != pbinAccountKeyLength || basicDataKey[pbinAccountKeyLength-1] != pbinBasicDataLeafKey {
		return fmt.Errorf("pbin: %x is not a BASIC_DATA key", basicDataKey)
	}
	copy(s.siblingKey[:], basicDataKey)
	s.siblingKey[pbinAccountKeyLength-1] = subIndex
	return s.emit(s.siblingKey[:], plainKey, update)
}
