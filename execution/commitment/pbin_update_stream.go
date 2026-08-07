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

	siblingKey   [pbinAccountKeyLength]byte
	pendingCode  pbinPendingCode
	overflowCode []pbinOverflowChunk
	keyDigest    pbinDigestCache

	// witness is what the parent state cannot tell a witness pass about the block.
	// See chunkSource and removesAccount.
	witness     PBinWitnessBlock
	witnessPass bool

	pendingRemoval []pbinAccountRemoval
}

// pbinAccountRemoval is a storage subtree waiting for the walk to reach its zone.
type pbinAccountRemoval struct {
	prefix   []byte
	plainKey [length.Addr]byte
}

type pbinPendingCode struct {
	stem     [pbinAccountKeyLength - 1]byte
	plainKey [length.Addr]byte
	chunks   [][pbinValueLength]byte
}

type pbinOverflowChunk struct {
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
	if err = s.flushPendingCode(); err != nil {
		return processed, err
	}
	if err = s.flushOverflowCode(); err != nil {
		return processed, err
	}
	if err = s.flushRemovals(nil); err != nil {
		return processed, err
	}
	return processed, nil
}

func (s *pbinUpdateStream) reset() {
	s.state, s.emit = nil, nil
	s.pendingCode = pbinPendingCode{}
	s.overflowCode = s.overflowCode[:0]
	s.pendingRemoval = s.pendingRemoval[:0]
}

func (s *pbinUpdateStream) release() {
	s.reset()
	s.keyDigest = pbinDigestCache{}
	s.witness = PBinWitnessBlock{}
}

// processKey expands an account into its basic-data and code-hash leaves. Code
// chunks are delayed until emitting them cannot move the ordered trie walk back.
func (s *pbinUpdateStream) processKey(treeKey, plainKey []byte, stateUpdate *Update) error {
	if err := s.flushPendingCodeBefore(treeKey); err != nil {
		return err
	}
	if err := s.flushOverflowCodeBefore(treeKey); err != nil {
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
		if err := s.removeAccount(plainKey, update); err != nil {
			return err
		}
	}
	if err := s.emit(treeKey, plainKey, update); err != nil {
		return err
	}
	if len(plainKey) != length.Addr {
		return nil
	}
	codeKey, err := s.codeHashKey(treeKey)
	if err != nil {
		return err
	}
	if err = s.emit(codeKey, plainKey, update); err != nil {
		return err
	}
	return s.queueCode(treeKey, plainKey, update)
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
// holds, which for storage nothing enumerates. Overflow code chunks stay: they
// are shared with every account running the same bytecode (eip:608-641).
func (s *pbinUpdateStream) removeAccount(plainKey []byte, update *Update) error {
	if err := s.emit(s.keyDigest.accountHeaderStem(plainKey), plainKey, update); err != nil {
		return err
	}
	s.pendingRemoval = append(s.pendingRemoval, pbinAccountRemoval{
		prefix: s.keyDigest.accountStoragePrefix(plainKey),
	})
	copy(s.pendingRemoval[len(s.pendingRemoval)-1].plainKey[:], plainKey)
	return nil
}

func (s *pbinUpdateStream) flushRemovalsBefore(treeKey []byte) error {
	if len(s.pendingRemoval) == 0 || treeKey[0] < pbinStorageZone {
		return nil
	}
	return s.flushRemovals(treeKey)
}

// flushRemovals emits the storage-prefix drops sorting before upTo, or all of
// them when upTo is nil. A drop has to land before any storage key it covers.
func (s *pbinUpdateStream) flushRemovals(upTo []byte) error {
	slices.SortFunc(s.pendingRemoval, func(a, b pbinAccountRemoval) int {
		return bytes.Compare(a.prefix, b.prefix)
	})
	kept := s.pendingRemoval[:0]
	for i := range s.pendingRemoval {
		r := &s.pendingRemoval[i]
		if upTo != nil && bytes.Compare(r.prefix, upTo) >= 0 {
			kept = append(kept, *r)
			continue
		}
		update := Update{Flags: DeleteUpdate}
		if err := s.emit(r.prefix, r.plainKey[:], &update); err != nil {
			return err
		}
	}
	s.pendingRemoval = kept
	return nil
}

// chunkSource is the code an account's chunk keys derive from, with the hash
// addressing its overflow chunks. A witness pass walks the parent state, where a
// contract the block creates has no code, so it needs the override to reach the
// same keys the fold did. Only key derivation moves; values stay pre-state.
func (s *pbinUpdateStream) chunkSource(plainKey []byte, update *Update) ([]byte, common.Hash, error) {
	if code, ok := s.witness.Code[string(plainKey)]; ok {
		return code, common.Hash(keccak.Sum256(code)), nil
	}
	if update.CodeSize == 0 {
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

func (s *pbinUpdateStream) queueCode(basicDataKey, plainKey []byte, update *Update) error {
	code, codeHash, err := s.chunkSource(plainKey, update)
	if err != nil {
		return err
	}
	if len(code) == 0 {
		return nil
	}
	if len(s.pendingCode.chunks) != 0 {
		return fmt.Errorf("pbin: code for %x queued while %x is still pending: the stem exit was missed",
			plainKey, s.pendingCode.plainKey[:])
	}
	chunks := pbinChunkifyCode(code)
	if len(chunks) > pbinHeaderCodeChunks {
		for i := pbinHeaderCodeChunks; i < len(chunks); i++ {
			var oc pbinOverflowChunk
			copy(oc.key[:], s.keyDigest.codeOverflowKey(codeHash, i))
			oc.value = chunks[i]
			s.overflowCode = append(s.overflowCode, oc)
		}
		chunks = chunks[:pbinHeaderCodeChunks]
	}
	s.pendingCode.chunks = chunks
	copy(s.pendingCode.stem[:], basicDataKey)
	copy(s.pendingCode.plainKey[:], plainKey)
	return nil
}

func (s *pbinUpdateStream) flushOverflowCodeBefore(treeKey []byte) error {
	if len(s.overflowCode) == 0 || treeKey[0] <= pbinCodeZone {
		return nil
	}
	return s.flushOverflowCode()
}

func (s *pbinUpdateStream) flushOverflowCode() error {
	if len(s.overflowCode) == 0 {
		return nil
	}
	slices.SortFunc(s.overflowCode, func(a, b pbinOverflowChunk) int { return bytes.Compare(a.key[:], b.key[:]) })

	var prev *pbinOverflowChunk
	for i := range s.overflowCode {
		oc := &s.overflowCode[i]
		if prev != nil && oc.key == prev.key {
			if oc.value != prev.value {
				return fmt.Errorf("pbin: code chunk %x carries two values", oc.key[:])
			}
			continue
		}
		update := Update{Flags: StorageUpdate, StorageLen: pbinValueLength, Storage: oc.value}
		if err := s.emit(oc.key[:], nil, &update); err != nil {
			return err
		}
		prev = oc
	}
	s.overflowCode = s.overflowCode[:0]
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

func (s *pbinUpdateStream) flushPendingCodeBefore(treeKey []byte) error {
	if len(s.pendingCode.chunks) == 0 || bytes.HasPrefix(treeKey, s.pendingCode.stem[:]) {
		return nil
	}
	return s.flushPendingCode()
}

func (s *pbinUpdateStream) flushPendingCode() error {
	p := &s.pendingCode
	var key [pbinAccountKeyLength]byte
	copy(key[:], p.stem[:])
	for i := range p.chunks {
		key[pbinAccountKeyLength-1] = byte(pbinCodeOffset + i)
		update := Update{Flags: StorageUpdate, StorageLen: pbinValueLength, Storage: p.chunks[i]}
		if err := s.emit(key[:], nil, &update); err != nil {
			return err
		}
	}
	p.chunks = nil
	return nil
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

func (s *pbinUpdateStream) codeHashKey(basicDataKey []byte) ([]byte, error) {
	if len(basicDataKey) != pbinAccountKeyLength || basicDataKey[pbinAccountKeyLength-1] != pbinBasicDataLeafKey {
		return nil, fmt.Errorf("pbin: %x is not a BASIC_DATA key", basicDataKey)
	}
	copy(s.siblingKey[:], basicDataKey)
	s.siblingKey[pbinAccountKeyLength-1] = pbinCodeHashLeafKey
	return s.siblingKey[:], nil
}
