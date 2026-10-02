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

package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/holiman/uint256"
	"github.com/urfave/cli/v3"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var errVerifyPBTInvalid = errors.New("verify-pbt: artifact rejected")

var verifyPBTCommand = cli.Command{
	Name:   "verify-pbt",
	Usage:  "Verify a PBT snapshot and preimages against the canonical MPT state",
	Action: doVerifyPBT,
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{Name: "snapshot", Usage: "PBT snapshot artifact", Required: true},
		&cli.StringFlag{Name: "preimages", Usage: "PBT preimages artifact", Required: true},
		&cli.Uint64Flag{Name: "block", Usage: "canonical anchor block", Required: true},
	}),
}

func doVerifyPBT(ctx context.Context, cliCtx *cli.Command) error {
	err := verifyPBTFiles(ctx, cliCtx.String(utils.DataDirFlag.Name), cliCtx.String("snapshot"), cliCtx.String("preimages"), cliCtx.Uint64("block"))
	if err == nil {
		return nil
	}
	_, _ = fmt.Fprintln(os.Stderr, err)
	if errors.Is(err, errVerifyPBTInvalid) {
		return cli.Exit(err, 1)
	}
	return cli.Exit(err, 2)
}

type verifyPBTState struct {
	root         common.Hash
	headers      map[common.Hash]artifact.Header
	code         map[string][]byte
	storage      map[string][]byte
	addresses    map[common.Hash]common.Address
	addressSlots map[common.Address]map[[32]byte]struct{}
	usedCode     map[string]struct{}
	codeSizes    map[common.Hash]uint64
}

func verifyPBTFiles(ctx context.Context, dataDir, snapshotPath, preimagesPath string, block uint64) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: malformed input: %v", errVerifyPBTInvalid, recovered)
		}
	}()

	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer snapshot.Close()
	preimages, err := os.Open(preimagesPath)
	if err != nil {
		return err
	}
	defer preimages.Close()
	snapshotInfo, err := snapshot.Stat()
	if err != nil {
		return err
	}
	preimageInfo, err := preimages.Stat()
	if err != nil {
		return err
	}

	state, err := readPBTVerificationState(snapshot, snapshotInfo.Size())
	if err != nil {
		return fmt.Errorf("%w: snapshot: %w", errVerifyPBTInvalid, err)
	}
	tmp, err := os.MkdirTemp("", "erigon-verify-pbt-")
	if err != nil {
		return err
	}
	defer func() { _ = dir.RemoveAll(tmp) }()
	if err := artifact.JoinAt(snapshot, snapshotInfo.Size(), preimages, preimageInfo.Size(), pbtVerifyHash, nil, tmp); err != nil {
		return fmt.Errorf("%w: preimages: %w", errVerifyPBTInvalid, err)
	}
	if err := readPBTVerificationPreimages(preimages, preimageInfo.Size(), state); err != nil {
		return fmt.Errorf("%w: preimages: %w", errVerifyPBTInvalid, err)
	}
	if err := verifyPBTCode(state); err != nil {
		return fmt.Errorf("%w: code: %w", errVerifyPBTInvalid, err)
	}
	root, err := verifyPBTMPT(state)
	if err != nil {
		return fmt.Errorf("%w: mpt: %w", errVerifyPBTInvalid, err)
	}
	if err := verifyPBTHeaderRoot(ctx, dataDir, block, root); err != nil {
		return err
	}
	return nil
}

func readPBTVerificationState(src io.ReaderAt, size int64) (*verifyPBTState, error) {
	state := &verifyPBTState{
		headers:      make(map[common.Hash]artifact.Header),
		code:         make(map[string][]byte),
		storage:      make(map[string][]byte),
		addresses:    make(map[common.Hash]common.Address),
		addressSlots: make(map[common.Address]map[[32]byte]struct{}),
		usedCode:     make(map[string]struct{}),
		codeSizes:    make(map[common.Hash]uint64),
	}
	builder, err := eip8297.NewStreamRootBuilder(pbtVerifyHash)
	if err != nil {
		return nil, err
	}
	meta, err := artifact.ReadSnapshotStreamAt(src, size, artifact.SnapshotStreamCallbacks{
		Header: func(header artifact.Header) error {
			state.headers[header.AddressHash] = header
			return verifyPBTHeaderLeaves(builder, header, state)
		},
		Code: func(group artifact.Group) error {
			for _, entry := range group.Entries {
				key := eip8297.TreeKey(eip8297.CodeZone, group.StemHash[:], entry.Index)
				state.code[string(key)] = bytes.Clone(entry.Value)
				if err := builder.Add(key, pbtVerifyCodeRootValue(entry.Value)); err != nil {
					return err
				}
			}
			return nil
		},
		Storage: func(address common.Hash, groups func(func(artifact.Group) error) error) error {
			return groups(func(group artifact.Group) error {
				for _, entry := range group.Entries {
					position := make([]byte, 64)
					copy(position, address[:])
					copy(position[32:], group.StemHash[:])
					key := eip8297.TreeKey(eip8297.StorageZone, position, entry.Index)
					value := pbtVerifyStorageValue(entry.Value)
					state.storage[string(key)] = value
					if err := builder.Add(key, value); err != nil {
						return err
					}
				}
				return nil
			})
		},
	})
	if err != nil {
		return nil, err
	}
	state.root, err = builder.RootHash()
	if err != nil {
		return nil, err
	}
	if state.root != meta.Root {
		return nil, fmt.Errorf("pbt root %x differs from trailer %x", state.root, meta.Root)
	}
	return state, nil
}

func verifyPBTHeaderLeaves(builder *eip8297.StreamRootBuilder, header artifact.Header, state *verifyPBTState) error {
	codeSize := bytesToUint64(header.CodeSize)
	if header.Kind == 2 {
		codeSize = eip8297.DelegationCodeLength
	}
	basic, err := eip8297.EncodeBasicData(bytesToUint64(header.Nonce), uint256.NewInt(0).SetBytes(header.Balance), codeSize)
	if err != nil {
		return err
	}
	if err := builder.Add(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.BasicDataLeafKey), basic[:]); err != nil {
		return err
	}
	switch header.Kind {
	case 0:
		emptyCodeHash := eip8297.CodeHashValue(common.Hash{})
		if err := builder.Add(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.CodeHashLeafKey), emptyCodeHash[:]); err != nil {
			return err
		}
	case 1:
		if err := builder.Add(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.CodeHashLeafKey), header.CodeHash[:]); err != nil {
			return err
		}
	case 2:
		var delegation [eip8297.ValueLength]byte
		copy(delegation[:], eip8297.DelegationMarker[:])
		copy(delegation[3:], header.Target[:])
		if err := builder.Add(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.DelegationLeafKey), delegation[:]); err != nil {
			return err
		}
	}
	for _, slot := range header.Slots {
		value := pbtVerifyStorageValue(slot.Value)
		key := eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.HeaderStorageOffset+slot.Index)
		state.storage[string(key)] = value
		if err := builder.Add(key, value); err != nil {
			return err
		}
	}
	return nil
}

func pbtVerifyCodeRootValue(value []byte) []byte {
	result := make([]byte, eip8297.ValueLength)
	copy(result[len(result)-len(value):], value)
	return result
}

func pbtVerifyStorageValue(value []byte) []byte {
	result := make([]byte, eip8297.ValueLength)
	copy(result[len(result)-len(value):], value)
	return result
}

func readPBTVerificationPreimages(src io.ReaderAt, size int64, state *verifyPBTState) error {
	return artifact.ReadPreimagesStream(src, size, func(address common.Address, slots func(func([32]byte) error) error) error {
		address32 := eip8297.RightAlign32(address[:])
		stem := pbtVerifyHash(address32[:])
		state.addresses[stem] = address
		if state.addressSlots[address] == nil {
			state.addressSlots[address] = make(map[[32]byte]struct{})
		}
		return slots(func(slot [32]byte) error {
			state.addressSlots[address][slot] = struct{}{}
			return nil
		})
	})
}

func verifyPBTCode(state *verifyPBTState) error {
	cache := eip8297.DigestCache{Sum: pbtVerifyHash}
	for stem := range state.headers {
		header := state.headers[stem]
		if header.Kind != 1 {
			continue
		}
		codeSize := bytesToUint64(header.CodeSize)
		if codeSize == 0 {
			return fmt.Errorf("code size is zero for %x", stem)
		}
		if previous, ok := state.codeSizes[header.CodeHash]; ok && previous != codeSize {
			return fmt.Errorf("code size for %x differs: %d and %d", header.CodeHash, previous, codeSize)
		}
		state.codeSizes[header.CodeHash] = codeSize
		chunks := (codeSize + eip8297.ChunkDataLen - 1) / eip8297.ChunkDataLen
		code := make([]byte, chunks*eip8297.ChunkDataLen)
		chunkValues := make([][]byte, chunks)
		for index := range chunks {
			key := cache.CodeChunkKey(header.CodeHash, int(index))
			value, ok := state.code[string(key)]
			if !ok {
				continue
			}
			chunkValues[index] = value
			state.usedCode[string(key)] = struct{}{}
			chunk := value
			if len(chunk) == eip8297.ValueLength {
				chunk = chunk[1:]
			}
			copy(code[index*eip8297.ChunkDataLen:], chunk)
		}
		code = code[:codeSize]
		if eip8297.IsDelegation(code) {
			return fmt.Errorf("kind-1 account %x contains a delegation indicator", stem)
		}
		if got := common.BytesToHash(crypto.Keccak256(code)); got != header.CodeHash {
			return fmt.Errorf("code hash mismatch for %x: account %x code %x", stem, header.CodeHash, got)
		}
		for index, expected := range eip8297.ChunkifyCode(code) {
			want := bytes.TrimLeft(expected[:], "\x00")
			if !bytes.Equal(chunkValues[index], want) {
				return fmt.Errorf("code chunk mismatch for %x at index %d", header.CodeHash, index)
			}
		}
	}
	for key := range state.code {
		if _, ok := state.usedCode[key]; !ok {
			return fmt.Errorf("surplus code group leaf %x", key)
		}
	}
	return nil
}

func verifyPBTMPT(state *verifyPBTState) (common.Hash, error) {
	accountsTrie := trie.NewInMemoryTrieRLPEncoded(nil)
	cache := eip8297.DigestCache{Sum: pbtVerifyHash}
	for stem := range state.headers {
		header := state.headers[stem]
		address, ok := state.addresses[stem]
		if !ok {
			return common.Hash{}, fmt.Errorf("missing address preimage for %x", stem)
		}
		var balance uint256.Int
		balance.SetBytes(header.Balance)
		account := accounts.Account{Nonce: bytesToUint64(header.Nonce), Balance: balance, Root: trie.EmptyRoot, CodeHash: accounts.EmptyCodeHash}
		slots := state.addressSlots[address]
		storageTrie := trie.NewInMemoryTrie(nil)
		for slot := range slots {
			var key []byte
			if eip8297.SlotInHeader(&slot) {
				key = eip8297.TreeKey(eip8297.AccountZone, stem[:], eip8297.HeaderStorageOffset+slot[31])
			} else {
				key = cache.StorageKey(address[:], slot[:])
			}
			value, ok := state.storage[string(key)]
			if !ok {
				return common.Hash{}, fmt.Errorf("missing storage value for %x", key)
			}
			storageTrie.Update(crypto.Keccak256(slot[:]), trimPBTValue(value))
		}
		if len(slots) != 0 {
			account.Root = storageTrie.Hash()
		}
		switch header.Kind {
		case 1:
			account.CodeHash = accounts.InternCodeHash(header.CodeHash)
		case 2:
			code := append(append([]byte(nil), eip8297.DelegationMarker[:]...), header.Target[:]...)
			account.CodeHash = accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(code)))
		}
		accountsTrie.Update(crypto.Keccak256(address[:]), account.RLP())
	}
	return accountsTrie.Hash(), nil
}

func trimPBTValue(value []byte) []byte {
	value = slices.Clone(value)
	return bytes.TrimLeft(value, "\x00")
}

func bytesToUint64(value []byte) uint64 {
	var result uint64
	for _, b := range value {
		result = result<<8 | uint64(b)
	}
	return result
}

func verifyPBTHeaderRoot(ctx context.Context, dataDir string, block uint64, root common.Hash) error {
	dirs := datadir.Open(dataDir)
	db, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return err
	}
	defer db.Close()
	var header *types.Header
	err = db.View(ctx, func(tx kv.Tx) error {
		canonicalHash, err := rawdb.ReadCanonicalHash(tx, block)
		if err != nil {
			return err
		}
		if canonicalHash == (common.Hash{}) {
			return nil
		}
		header = rawdb.ReadHeader(tx, canonicalHash, block)
		return nil
	})
	if err != nil {
		return err
	}
	if header == nil {
		return fmt.Errorf("verify-pbt: block %d header is missing", block)
	}
	if header.Root != root {
		return fmt.Errorf("%w: artifact MPT root %x differs from header root %x", errVerifyPBTInvalid, root, header.Root)
	}
	return nil
}

func pbtVerifyHash(value []byte) common.Hash { return common.Hash(blake3.Sum256(value)) }
