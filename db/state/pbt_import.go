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

package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type PBTImportOptions struct {
	Snapshot     io.ReaderAt
	SnapshotSize int64
	Preimages    io.ReaderAt
	PreimageSize int64
	BlockHash    common.Hash
	BlockNum     uint64
	TxNum        uint64
	Hash         eip8297.HashFn
	Logger       log.Logger
}

type pbtImportState struct {
	meta      artifact.SnapshotMeta
	headers   map[common.Hash]artifact.Header
	addresses map[common.Hash]common.Address
	slots     map[common.Address]map[[32]byte][]byte
	code      map[common.Hash]map[byte][]byte
	storage   map[string][]byte
}

func ImportPBTSnapshot(ctx context.Context, tx kv.TemporalRwTx, opts PBTImportOptions) (common.Hash, error) {
	if tx == nil || opts.Snapshot == nil || opts.Preimages == nil {
		return common.Hash{}, fmt.Errorf("pbt import: missing input")
	}
	if opts.Hash == nil {
		return common.Hash{}, fmt.Errorf("pbt import: nil hash function")
	}
	if opts.Logger == nil {
		opts.Logger = log.Root()
	}
	state, err := readPBTImportState(opts)
	if err != nil {
		return common.Hash{}, err
	}
	if validationErr := validatePBTImportState(state, opts.Hash); validationErr != nil {
		return common.Hash{}, validationErr
	}
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(ctx, tx, opts.Logger, execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentDomain), execctx.WithoutCommitmentSeek())
	if err != nil {
		return common.Hash{}, err
	}
	defer domains.Close()
	feed := &commitment.PBinFeed{}
	addresses := make([]common.Hash, 0, len(state.headers))
	for addressHash := range state.headers {
		addresses = append(addresses, addressHash)
	}
	sort.Slice(addresses, func(i, j int) bool { return bytes.Compare(addresses[i][:], addresses[j][:]) < 0 })
	feed.Accounts = make([]commitment.PBinFeedAccount, 0, len(addresses))
	for _, addressHash := range addresses {
		header := state.headers[addressHash]
		address := state.addresses[addressHash]
		code, codeHash, importErr := importCodeForHeader(header, state.code, opts.Hash)
		if importErr != nil {
			return common.Hash{}, importErr
		}
		balance := new(uint256.Int).SetBytes(header.Balance)
		account := accounts.Account{
			Nonce:       integerUint64(header.Nonce),
			Balance:     *balance,
			CodeHash:    accounts.InternCodeHash(codeHash),
			Incarnation: 0,
		}
		if len(code) != 0 {
			account.Incarnation = 1
		}
		if putErr := domains.DomainPut(kv.AccountsDomain, tx, address[:], accounts.SerialiseV3(&account), opts.TxNum, nil); putErr != nil {
			return common.Hash{}, putErr
		}
		if len(code) != 0 {
			if putErr := domains.DomainPut(kv.CodeDomain, tx, address[:], code, opts.TxNum, nil); putErr != nil {
				return common.Hash{}, putErr
			}
		}
		feedAccount := commitment.PBinFeedAccount{
			Address:     append([]byte(nil), address[:]...),
			Exists:      true,
			Nonce:       account.Nonce,
			Balance:     account.Balance,
			CodeHash:    codeHash,
			CodeWritten: true,
			Code:        append([]byte(nil), code...),
		}
		for slot, value := range state.slots[address] {
			feedAccount.Slots = append(feedAccount.Slots, commitment.PBinFeedSlot{Key: append([]byte(nil), slot[:]...), Value: append([]byte(nil), value...)})
			storageKey := make([]byte, 0, len(address)+len(slot))
			storageKey = append(storageKey, address[:]...)
			storageKey = append(storageKey, slot[:]...)
			if putErr := domains.DomainPut(kv.StorageDomain, tx, storageKey, value, opts.TxNum, nil); putErr != nil {
				return common.Hash{}, putErr
			}
		}
		sort.Slice(feedAccount.Slots, func(i, j int) bool { return bytes.Compare(feedAccount.Slots[i].Key, feedAccount.Slots[j].Key) < 0 })
		feed.Accounts = append(feed.Accounts, feedAccount)
	}
	commitmentCtx := domains.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	if commitmentCtx == nil {
		return common.Hash{}, fmt.Errorf("pbt import: binary commitment domain is not registered")
	}
	commitmentCtx.SetPBinFeed(feed)
	rootBytes, err := commitmentCtx.ComputeCommitment(ctx, tx, true, opts.BlockNum, opts.TxNum, "pbt-import", nil)
	if err != nil {
		return common.Hash{}, err
	}
	root := common.BytesToHash(rootBytes)
	if root != state.meta.Root {
		return common.Hash{}, fmt.Errorf("pbt import: computed root %s differs from artifact root %s", root.Hex(), state.meta.Root.Hex())
	}
	header := rawdb.ReadHeader(tx, opts.BlockHash, opts.BlockNum)
	if header == nil {
		return common.Hash{}, fmt.Errorf("pbt import: local header %s is missing", opts.BlockHash.Hex())
	}
	if root != header.Root {
		return common.Hash{}, fmt.Errorf("pbt import: computed root %s differs from header root %s", root.Hex(), header.Root.Hex())
	}
	return root, nil
}

func readPBTImportState(opts PBTImportOptions) (*pbtImportState, error) {
	result := &pbtImportState{
		headers:   make(map[common.Hash]artifact.Header),
		addresses: make(map[common.Hash]common.Address),
		slots:     make(map[common.Address]map[[32]byte][]byte),
		code:      make(map[common.Hash]map[byte][]byte),
		storage:   make(map[string][]byte),
	}
	meta, err := artifact.ReadSnapshotStreamAt(opts.Snapshot, opts.SnapshotSize, artifact.SnapshotStreamCallbacks{
		Header: func(header artifact.Header) error {
			result.headers[header.AddressHash] = header
			return nil
		},
		Code: func(group artifact.Group) error {
			entries := result.code[group.StemHash]
			if entries == nil {
				entries = make(map[byte][]byte)
				result.code[group.StemHash] = entries
			}
			for _, entry := range group.Entries {
				entries[entry.Index] = append([]byte(nil), entry.Value...)
			}
			return nil
		},
		Storage: func(addressHash common.Hash, groups func(func(artifact.Group) error) error) error {
			return groups(func(group artifact.Group) error {
				for _, entry := range group.Entries {
					key := eip8297.TreeKey(eip8297.StorageZone, append(append([]byte(nil), addressHash[:]...), group.StemHash[:]...), entry.Index)
					result.storage[string(key)] = append([]byte(nil), entry.Value...)
				}
				return nil
			})
		},
	})
	if err != nil {
		return nil, fmt.Errorf("pbt import: read snapshot: %w", err)
	}
	result.meta = meta
	if joinErr := artifact.JoinAt(opts.Snapshot, opts.SnapshotSize, opts.Preimages, opts.PreimageSize, opts.Hash, nil); joinErr != nil {
		return nil, fmt.Errorf("pbt import: join preimages: %w", joinErr)
	}
	err = artifact.ReadPreimagesStream(opts.Preimages, opts.PreimageSize, func(address common.Address, slots func(func([32]byte) error) error) error {
		address32 := eip8297.RightAlign32(address[:])
		addressHash := opts.Hash(address32[:])
		result.addresses[addressHash] = address
		if result.slots[address] == nil {
			result.slots[address] = make(map[[32]byte][]byte)
		}
		return slots(func(slot [32]byte) error {
			result.slots[address][slot] = nil
			if eip8297.SlotInHeader(&slot) {
				header, ok := result.headers[addressHash]
				if !ok {
					return fmt.Errorf("pbt import: preimage address %x has no header", address)
				}
				for _, headerSlot := range header.Slots {
					if headerSlot.Index == slot[31] {
						result.slots[address][slot] = append([]byte(nil), headerSlot.Value...)
						return nil
					}
				}
				return fmt.Errorf("pbt import: header slot %d has no value", slot[31])
			}
			key := eip8297.TreeKeyStorage(address32[:], slot[:])
			value, ok := result.storage[string(key)]
			if !ok {
				return fmt.Errorf("pbt import: storage preimage %x has no artifact value", slot)
			}
			result.slots[address][slot] = append([]byte(nil), value...)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("pbt import: read preimages: %w", err)
	}
	return result, nil
}

func validatePBTImportState(state *pbtImportState, hashFn eip8297.HashFn) error {
	usedGroups := make(map[common.Hash]struct{})
	codeSizes := make(map[common.Hash]uint64)
	for addressHash := range state.headers {
		header := state.headers[addressHash]
		if header.Kind != 1 {
			continue
		}
		size := integerUint64(header.CodeSize)
		if old, exists := codeSizes[header.CodeHash]; exists && old != size {
			return fmt.Errorf("pbt import: code size disagrees for code hash %x", header.CodeHash)
		}
		codeSizes[header.CodeHash] = size
	}
	for addressHash := range state.headers {
		header := state.headers[addressHash]
		address, ok := state.addresses[addressHash]
		if !ok {
			return fmt.Errorf("pbt import: header %x has no address preimage", addressHash)
		}
		address32 := eip8297.RightAlign32(address[:])
		if hashFn(address32[:]) != addressHash {
			return fmt.Errorf("pbt import: address preimage %x hashes to the wrong header", address)
		}
		switch header.Kind {
		case 1:
			size := integerUint64(header.CodeSize)
			if size == 0 {
				return fmt.Errorf("pbt import: code size is zero for %x", address)
			}
			code, _, codeErr := importCodeForHeader(header, state.code, hashFn)
			if codeErr != nil {
				return codeErr
			}
			for _, stem := range pbtCodeGroupStems(header.CodeHash, integerUint64(header.CodeSize), hashFn) {
				usedGroups[stem] = struct{}{}
			}
			if eip8297.IsDelegation(code) {
				return fmt.Errorf("pbt import: kind 1 account %x contains a delegation designator", address)
			}
		case 0, 2:
		default:
			return fmt.Errorf("pbt import: unknown account kind %d", header.Kind)
		}
	}
	for stem := range state.code {
		if _, ok := usedGroups[stem]; !ok {
			return fmt.Errorf("pbt import: surplus code group %x", stem)
		}
	}
	return nil
}

func importCodeForHeader(header artifact.Header, groups map[common.Hash]map[byte][]byte, hashFn eip8297.HashFn) ([]byte, common.Hash, error) {
	switch header.Kind {
	case 0:
		return nil, empty.CodeHash, nil
	case 2:
		code := append(append([]byte(nil), eip8297.DelegationMarker[:]...), header.Target[:]...)
		return code, common.Hash(keccak.Sum256(code)), nil
	case 1:
		size := integerUint64(header.CodeSize)
		count := (size + eip8297.ChunkDataLen - 1) / eip8297.ChunkDataLen
		stems := pbtCodeGroupStems(header.CodeHash, size, hashFn)
		stemIndexes := make(map[common.Hash]uint64, len(stems))
		for groupIndex, stem := range stems {
			stemIndexes[stem] = uint64(groupIndex)
		}
		for stem, groupIndex := range stemIndexes {
			entries := groups[stem]
			for index := range entries {
				if groupIndex*eip8297.StemSubtreeWidth+uint64(index) >= count {
					return nil, common.Hash{}, fmt.Errorf("pbt import: surplus code chunk %d for %x", groupIndex*eip8297.StemSubtreeWidth+uint64(index), header.CodeHash)
				}
			}
		}
		code := make([]byte, count*eip8297.ChunkDataLen)
		chunks := make([][eip8297.ValueLength]byte, count)
		for index := range count {
			groupIndex := index / eip8297.StemSubtreeWidth
			stem := pbtCodeGroupStem(hashFn, header.CodeHash, groupIndex)
			entries := groups[stem]
			if value, ok := entries[byte(index%eip8297.StemSubtreeWidth)]; ok {
				if len(value) > eip8297.ValueLength {
					return nil, common.Hash{}, fmt.Errorf("pbt import: code chunk %d has invalid width", index)
				}
				copy(chunks[index][eip8297.ValueLength-len(value):], value)
			}
			copy(code[index*eip8297.ChunkDataLen:], chunks[index][1:])
		}
		code = code[:size]
		actual := eip8297.ChunkifyCode(code)
		if len(actual) != len(chunks) || !bytes.Equal(flattenChunks(actual), flattenChunks(chunks)) {
			return nil, common.Hash{}, fmt.Errorf("pbt import: code chunks disagree for %x", header.CodeHash)
		}
		codeHash := common.Hash(keccak.Sum256(code))
		if codeHash != header.CodeHash {
			return nil, common.Hash{}, fmt.Errorf("pbt import: code hash mismatch for %x", header.CodeHash)
		}
		return code, codeHash, nil
	default:
		return nil, common.Hash{}, fmt.Errorf("pbt import: unknown account kind %d", header.Kind)
	}
}

func pbtCodeGroupStems(codeHash common.Hash, codeSize uint64, hashFn eip8297.HashFn) []common.Hash {
	count := (codeSize + eip8297.ChunkDataLen - 1) / eip8297.ChunkDataLen
	result := make([]common.Hash, 0, (count+eip8297.StemSubtreeWidth-1)/eip8297.StemSubtreeWidth)
	for groupIndex := uint64(0); groupIndex*eip8297.StemSubtreeWidth < count; groupIndex++ {
		result = append(result, pbtCodeGroupStem(hashFn, codeHash, groupIndex))
	}
	return result
}

func pbtCodeGroupStem(hashFn eip8297.HashFn, codeHash common.Hash, groupIndex uint64) common.Hash {
	var input [64]byte
	copy(input[:32], codeHash[:])
	binary.BigEndian.PutUint64(input[56:], groupIndex)
	return hashFn(input[:])
}

func flattenChunks(chunks [][eip8297.ValueLength]byte) []byte {
	result := make([]byte, 0, len(chunks)*eip8297.ValueLength)
	for _, chunk := range chunks {
		result = append(result, chunk[:]...)
	}
	return result
}

func integerUint64(value []byte) uint64 {
	if len(value) > 8 {
		return math.MaxUint64
	}
	var raw [8]byte
	copy(raw[8-len(value):], value)
	var result uint64
	for _, b := range raw {
		result = result<<8 | uint64(b)
	}
	return result
}
