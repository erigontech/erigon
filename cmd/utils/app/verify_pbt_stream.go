// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package app

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var errPBTVerifyScratchIO = errors.New("verify-pbt: scratch I/O")

type pbtVerifyProgress struct {
	logger log.Logger
	msg    string
	phase  string
	count  uint64
	next   time.Time
}

func (p *pbtVerifyProgress) add(key []byte) {
	p.count++
	now := time.Now()
	if now.Before(p.next) {
		return
	}
	p.next = now.Add(30 * time.Second)
	p.logger.Info(p.msg, "phase", p.phase, "records", p.count, "key_prefix", hex.EncodeToString(key[:min(len(key), 8)]))
}

type pbtVerifyAccountRecord struct {
	Nonce    uint64
	Balance  []byte
	Root     common.Hash
	CodeHash common.Hash
}

type pbtVerifyKVReader struct {
	r    *bufio.Reader
	path string
}

func (r *pbtVerifyKVReader) next() ([]byte, []byte, bool, error) {
	var lengths [8]byte
	if _, err := io.ReadFull(r.r, lengths[:]); err != nil {
		if err == io.EOF {
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("%w: %s: %w", errPBTVerifyScratchIO, r.path, err)
	}
	key := make([]byte, binary.BigEndian.Uint32(lengths[:4]))
	value := make([]byte, binary.BigEndian.Uint32(lengths[4:]))
	if _, err := io.ReadFull(r.r, key); err != nil {
		return nil, nil, false, fmt.Errorf("%w: %s: %w", errPBTVerifyScratchIO, r.path, err)
	}
	if _, err := io.ReadFull(r.r, value); err != nil {
		return nil, nil, false, fmt.Errorf("%w: %s: %w", errPBTVerifyScratchIO, r.path, err)
	}
	return key, value, true, nil
}

func pbtVerifyWriteKV(w *bufio.Writer, key, value []byte) error {
	var lengths [8]byte
	binary.BigEndian.PutUint32(lengths[:4], uint32(len(key)))
	binary.BigEndian.PutUint32(lengths[4:], uint32(len(value)))
	if _, err := w.Write(lengths[:]); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}

func pbtVerifyOpenKV(path string) (*os.File, *pbtVerifyKVReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return f, &pbtVerifyKVReader{r: bufio.NewReaderSize(f, 1<<20), path: path}, nil
}

func pbtVerifyNewCollector(name, scratch string) *etl.Collector {
	return etl.NewCollector(name, scratch, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()).SortAndFlushInBackground(true)
}

func pbtVerifyFlushCollector(collector *etl.Collector, path, phase string) error {
	f, err := os.Create(path)
	if err != nil {
		collector.Close()
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	progress := pbtVerifyProgress{logger: log.Root(), msg: "PBT verify progress", phase: phase, next: time.Now().Add(30 * time.Second)}
	err = collector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		progress.add(key)
		return pbtVerifyWriteKV(w, key, value)
	}, etl.TransformArgs{})
	collector.Close()
	if err == nil {
		err = w.Flush()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

func pbtVerifyEncode(value any) ([]byte, error) {
	var buffer bytes.Buffer
	err := gob.NewEncoder(&buffer).Encode(value)
	return buffer.Bytes(), err
}

func pbtVerifyDecode(value []byte, target any) error {
	return gob.NewDecoder(bytes.NewReader(value)).Decode(target)
}

func verifyPBTStreamingState(snapshot io.ReaderAt, snapshotSize int64, preimages io.ReaderAt, preimageSize int64, scratch string, maxCodeSize uint64) (common.Hash, error) {
	headers, err := os.Create(filepath.Join(scratch, "headers.bin"))
	if err != nil {
		return common.Hash{}, fmt.Errorf("read snapshot: %w", err)
	}
	defer headers.Close()
	headerEncoder := gob.NewEncoder(headers)
	leaves := pbtVerifyNewCollector("pbt-verify-leaves", scratch)
	codeActual := pbtVerifyNewCollector("pbt-verify-code-actual", scratch)
	codeRequirements := pbtVerifyNewCollector("pbt-verify-code-requirements", scratch)
	addresses := pbtVerifyNewCollector("pbt-verify-addresses", scratch)
	joinedSlots := pbtVerifyNewCollector("pbt-verify-slots", scratch)
	collectors := []*etl.Collector{leaves, codeActual, codeRequirements, addresses, joinedSlots}
	defer func() {
		for _, collector := range collectors {
			collector.Close()
		}
	}()
	cache := eip8297.DigestCache{Sum: pbtVerifyHash}
	meta, err := artifact.ReadSnapshotStreamAt(snapshot, snapshotSize, artifact.SnapshotStreamCallbacks{
		Header: func(header artifact.Header) error {
			if header.Kind == 1 {
				codeSize := dbstate.PBinIntegerUint64(header.CodeSize)
				if codeSize > maxCodeSize {
					return fmt.Errorf("%w: code size %d exceeds --max-code-size=%d", errVerifyPBTConfig, codeSize, maxCodeSize)
				}
			}
			if err := headerEncoder.Encode(header); err != nil {
				return err
			}
			if header.Kind == 1 {
				var codeSize [8]byte
				binary.BigEndian.PutUint64(codeSize[:], dbstate.PBinIntegerUint64(header.CodeSize))
				if err := codeRequirements.Collect(header.CodeHash[:], codeSize[:]); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		return common.Hash{}, fmt.Errorf("snapshot records: %w", err)
	}
	if err := headers.Sync(); err != nil {
		return common.Hash{}, err
	}
	leafProgress := pbtVerifyProgress{logger: log.Root(), msg: "PBT verify progress", phase: "snapshot leaves", next: time.Now()}
	root, err := dbstate.ForEachPBinArtifactLeaf(snapshot, snapshotSize, pbtVerifyHash, func(leaf dbstate.PBinLeaf) error {
		leafProgress.add(leaf.Key)
		if err := leaves.Collect(leaf.Key, leaf.Value); err != nil {
			return err
		}
		if leaf.Key[0] == eip8297.CodeZone {
			if err := codeActual.Collect(leaf.Key, leaf.Value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return common.Hash{}, fmt.Errorf("snapshot leaf stream: %w", err)
	}
	if root != meta.Root {
		return common.Hash{}, fmt.Errorf("pbt root %x differs from trailer %x", root, meta.Root)
	}
	if err := pbtVerifyCollectAddresses(preimages, preimageSize, addresses); err != nil {
		return common.Hash{}, fmt.Errorf("preimage records: %w", err)
	}
	joinProgress := pbtVerifyProgress{logger: log.Root(), msg: "PBT verify progress", phase: "preimage join", next: time.Now()}
	if err := artifact.JoinAt(snapshot, snapshotSize, preimages, preimageSize, pbtVerifyHash, func(address common.Address, slot [32]byte) error {
		joinProgress.add(address[:])
		return joinedSlots.Collect(cache.StorageKey(address[:], slot[:]), append(append([]byte{}, address[:]...), slot[:]...))
	}, scratch); err != nil {
		return common.Hash{}, fmt.Errorf("join preimages: %w", err)
	}
	paths := make([]string, 5)
	for i, pair := range []struct {
		collector *etl.Collector
		name      string
	}{
		{leaves, "leaves"},
		{codeActual, "code-actual"},
		{codeRequirements, "code-requirements"},
		{addresses, "addresses"},
		{joinedSlots, "joined-slots"},
	} {
		paths[i] = filepath.Join(scratch, pair.name+".sorted")
		if err := pbtVerifyFlushCollector(pair.collector, paths[i], "verify "+pair.name); err != nil {
			return common.Hash{}, err
		}
	}
	codeExpectedPath := filepath.Join(scratch, "code-expected.sorted")
	if err := pbtVerifyGenerateCodeExpected(paths[2], codeExpectedPath, scratch); err != nil {
		return common.Hash{}, err
	}
	if err := pbtVerifyCode(paths[1], codeExpectedPath, scratch); err != nil {
		return common.Hash{}, err
	}
	return pbtVerifyMPT(paths[0], paths[4], paths[3], headers.Name(), scratch)
}

func pbtVerifyCollectAddresses(src io.ReaderAt, size int64, collector *etl.Collector) error {
	return artifact.ReadPreimagesStream(src, size, func(address common.Address, slots func(func([32]byte) error) error) error {
		address32 := eip8297.RightAlign32(address[:])
		stem := pbtVerifyHash(address32[:])
		if err := collector.Collect(stem[:], address[:]); err != nil {
			return err
		}
		return slots(func([32]byte) error { return nil })
	})
}

func pbtVerifyGenerateCodeExpected(requirementPath, expectedPath, scratch string) error {
	requirementsFile, requirementsReader, err := pbtVerifyOpenKV(requirementPath)
	if err != nil {
		return err
	}
	defer requirementsFile.Close()
	expected := pbtVerifyNewCollector("pbt-verify-code-expected", scratch)
	defer expected.Close()
	var lastHash []byte
	var lastSize uint64
	for {
		codeHash, value, ok, err := requirementsReader.next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if len(codeHash) != length.Hash || len(value) != 8 {
			return fmt.Errorf("invalid code requirement")
		}
		codeSize := binary.BigEndian.Uint64(value)
		if bytes.Equal(lastHash, codeHash) {
			if codeSize != lastSize {
				return fmt.Errorf("code size disagreement for %x", codeHash)
			}
			continue
		}
		lastHash = bytes.Clone(codeHash)
		lastSize = codeSize
		if codeSize == 0 {
			return fmt.Errorf("code size is zero for %x", codeHash)
		}
		chunks := (codeSize + eip8297.ChunkDataLen - 1) / eip8297.ChunkDataLen
		currentCodeHash := common.BytesToHash(codeHash)
		for index := range chunks {
			key := (&eip8297.DigestCache{Sum: pbtVerifyHash}).CodeChunkKey(currentCodeHash, int(index))
			metadata := make([]byte, 48)
			copy(metadata, codeHash)
			binary.BigEndian.PutUint64(metadata[32:], codeSize)
			binary.BigEndian.PutUint64(metadata[40:], index)
			if err := expected.Collect(key, metadata); err != nil {
				return err
			}
		}
	}
	return pbtVerifyFlushCollector(expected, expectedPath, "verify code expected")
}

func pbtVerifyCode(actualPath, requirementPath, scratch string) error {
	requirementFile, requirementReader, err := pbtVerifyOpenKV(requirementPath)
	if err != nil {
		return err
	}
	defer requirementFile.Close()
	actualFile, actualReader, err := pbtVerifyOpenKV(actualPath)
	if err != nil {
		return err
	}
	defer actualFile.Close()
	codeRows := pbtVerifyNewCollector("pbt-verify-code-rows", scratch)
	defer codeRows.Close()
	codeProgress := pbtVerifyProgress{logger: log.Root(), msg: "PBT verify progress", phase: "code check", next: time.Now()}
	wantKey, wantValue, wantOK, err := requirementReader.next()
	if err != nil {
		return err
	}
	gotKey, gotValue, gotOK, err := actualReader.next()
	if err != nil {
		return err
	}
	for wantOK || gotOK {
		if !wantOK {
			return fmt.Errorf("surplus code group leaf %x", gotKey)
		}
		codeProgress.add(wantKey)
		if !gotOK || bytes.Compare(wantKey, gotKey) < 0 {
			if err := pbtVerifyCollectCodeRow(codeRows, wantKey, wantValue, nil); err != nil {
				return err
			}
			wantKey, wantValue, wantOK, err = requirementReader.next()
			if err != nil {
				return err
			}
			continue
		}
		if bytes.Compare(wantKey, gotKey) > 0 {
			return fmt.Errorf("surplus code group leaf %x before expected %x", gotKey, wantKey)
		}
		if err := pbtVerifyCollectCodeRow(codeRows, wantKey, wantValue, gotValue); err != nil {
			return err
		}
		wantKey, wantValue, wantOK, err = requirementReader.next()
		if err != nil {
			return err
		}
		gotKey, gotValue, gotOK, err = actualReader.next()
		if err != nil {
			return err
		}
	}
	if err := pbtVerifyFlushCollector(codeRows, filepath.Join(scratch, "code-rows.sorted"), "verify code"); err != nil {
		return err
	}
	return pbtVerifyCheckCodeRows(filepath.Join(scratch, "code-rows.sorted"))
}

func pbtVerifyCollectCodeRow(collector *etl.Collector, key, metadata, chunk []byte) error {
	if len(metadata) != 48 || len(key) != eip8297.CodeKeyLength {
		return fmt.Errorf("invalid code requirement for %x", key)
	}
	rowValue := make([]byte, 8, 8+len(chunk))
	copy(rowValue, metadata[32:40])
	rowValue = append(rowValue, chunk...)
	codeHash := common.BytesToHash(metadata[:32])
	index := binary.BigEndian.Uint64(metadata[40:])
	rowKey := append(append([]byte{}, codeHash[:]...), make([]byte, 8)...)
	binary.BigEndian.PutUint64(rowKey[32:], index)
	return collector.Collect(rowKey, rowValue)
}

func pbtVerifyCheckCodeRows(path string) error {
	f, reader, err := pbtVerifyOpenKV(path)
	if err != nil {
		return err
	}
	defer f.Close()
	key, value, ok, err := reader.next()
	if err != nil {
		return err
	}
	for ok {
		if len(key) != 40 || len(value) < 8 || uint64(len(value)-8) > eip8297.ValueLength {
			return fmt.Errorf("invalid code row")
		}
		codeHash := common.BytesToHash(key[:32])
		codeSize := binary.BigEndian.Uint64(value[:8])
		chunks := (codeSize + eip8297.ChunkDataLen - 1) / eip8297.ChunkDataLen
		var code []byte
		actualChunks := make([][eip8297.ValueLength]byte, 0, chunks)
		var codeRead uint64
		for ok && bytes.Equal(key[:32], codeHash[:]) {
			index := binary.BigEndian.Uint64(key[32:])
			if index >= chunks || index != codeRead/eip8297.ChunkDataLen {
				return fmt.Errorf("code chunks for %x are out of order", codeHash)
			}
			data := value[8:]
			var full [eip8297.ValueLength]byte
			copy(full[:], data)
			actualChunks = append(actualChunks, full)
			if codeRead >= codeSize {
				return fmt.Errorf("code chunks for %x exceed code size", codeHash)
			}
			written := min(uint64(eip8297.ChunkDataLen), codeSize-codeRead)
			code = append(code, full[1:1+written]...)
			codeRead += written
			key, value, ok, err = reader.next()
			if err != nil {
				return err
			}
		}
		if codeRead != codeSize {
			return fmt.Errorf("code chunks for %x end at %d, want %d", codeHash, codeRead, codeSize)
		}
		got := crypto.Keccak256Hash(code)
		if got != codeHash {
			return fmt.Errorf("code hash mismatch: account %x code %x", codeHash, got)
		}
		wantChunks := eip8297.ChunkifyCode(code)
		if len(wantChunks) != len(actualChunks) {
			return fmt.Errorf("code chunks for %x do not match code size", codeHash)
		}
		for index := range wantChunks {
			if wantChunks[index] != actualChunks[index] {
				return fmt.Errorf("code chunk %x at index %d does not match re-chunked code", codeHash, index)
			}
		}
		if codeSize == eip8297.DelegationCodeLength && len(code) >= len(eip8297.DelegationMarker) && bytes.Equal(code[:len(eip8297.DelegationMarker)], eip8297.DelegationMarker[:]) {
			return fmt.Errorf("kind-1 account %x contains a delegation indicator", codeHash)
		}
	}
	return nil
}

func pbtVerifyMPT(leavesPath, slotsPath, addressesPath, headersPath, scratch string) (common.Hash, error) {
	leafFile, leafReader, err := pbtVerifyOpenKV(leavesPath)
	if err != nil {
		return common.Hash{}, err
	}
	defer leafFile.Close()
	slotFile, slotReader, err := pbtVerifyOpenKV(slotsPath)
	if err != nil {
		return common.Hash{}, err
	}
	defer slotFile.Close()
	storageRows := pbtVerifyNewCollector("pbt-verify-mpt-storage", scratch)
	defer storageRows.Close()
	leafKey, leafValue, leafOK, err := leafReader.next()
	if err != nil {
		return common.Hash{}, err
	}
	slotKey, slotValue, slotOK, err := slotReader.next()
	if err != nil {
		return common.Hash{}, err
	}
	for slotOK {
		for leafOK && bytes.Compare(leafKey, slotKey) < 0 {
			leafKey, leafValue, leafOK, err = leafReader.next()
			if err != nil {
				return common.Hash{}, err
			}
		}
		if !leafOK || !bytes.Equal(leafKey, slotKey) {
			return common.Hash{}, fmt.Errorf("missing joined leaf %x", slotKey)
		}
		if len(slotValue) != length.Addr+length.Hash || len(leafValue) != eip8297.ValueLength {
			return common.Hash{}, fmt.Errorf("invalid joined storage row")
		}
		accountKey := crypto.Keccak256(slotValue[:length.Addr])
		storageKey := crypto.Keccak256(slotValue[length.Addr:])
		key := append(append(append([]byte{}, accountKey...), make([]byte, length.Incarnation)...), storageKey...)
		if err := storageRows.Collect(key, bytes.TrimLeft(leafValue, "\x00")); err != nil {
			return common.Hash{}, err
		}
		slotKey, slotValue, slotOK, err = slotReader.next()
		if err != nil {
			return common.Hash{}, err
		}
	}
	storagePath := filepath.Join(scratch, "mpt-storage.sorted")
	if err := pbtVerifyFlushCollector(storageRows, storagePath, "verify storage"); err != nil {
		return common.Hash{}, err
	}
	addressFile, addressReader, err := pbtVerifyOpenKV(addressesPath)
	if err != nil {
		return common.Hash{}, err
	}
	defer addressFile.Close()
	headerFile, err := os.Open(headersPath)
	if err != nil {
		return common.Hash{}, err
	}
	defer headerFile.Close()
	headerDecoder := gob.NewDecoder(headerFile)
	accountFacts := pbtVerifyNewCollector("pbt-verify-mpt-accounts", scratch)
	defer accountFacts.Close()
	addressKey, addressValue, addressOK, err := addressReader.next()
	if err != nil {
		return common.Hash{}, err
	}
	for {
		var record artifact.Header
		if err := headerDecoder.Decode(&record); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return common.Hash{}, fmt.Errorf("%w: %s: %w", errPBTVerifyScratchIO, headersPath, err)
		}
		for addressOK && bytes.Compare(addressKey, record.AddressHash[:]) < 0 {
			addressKey, addressValue, addressOK, err = addressReader.next()
			if err != nil {
				return common.Hash{}, err
			}
		}
		if !addressOK || !bytes.Equal(addressKey, record.AddressHash[:]) || len(addressValue) != length.Addr {
			return common.Hash{}, fmt.Errorf("missing address preimage for %x", record.AddressHash)
		}
		var address common.Address
		copy(address[:], addressValue)
		accountKey := crypto.Keccak256(address[:])
		encoded, err := pbtVerifyEncode(record)
		if err != nil {
			return common.Hash{}, err
		}
		if err := accountFacts.Collect(accountKey, encoded); err != nil {
			return common.Hash{}, err
		}
		addressKey, addressValue, addressOK, err = addressReader.next()
		if err != nil {
			return common.Hash{}, err
		}
	}
	if addressOK {
		return common.Hash{}, fmt.Errorf("surplus address row")
	}
	accountFactsPath := filepath.Join(scratch, "mpt-accounts.sorted")
	if err := pbtVerifyFlushCollector(accountFacts, accountFactsPath, "verify MPT accounts"); err != nil {
		return common.Hash{}, err
	}
	accountRows := pbtVerifyNewCollector("pbt-verify-mpt-account-rows", scratch)
	defer accountRows.Close()
	if err := pbtVerifyBuildAccountRows(accountFactsPath, storagePath, accountRows); err != nil {
		return common.Hash{}, err
	}
	accountRowsPath := filepath.Join(scratch, "mpt-account-rows.sorted")
	if err := pbtVerifyFlushCollector(accountRows, accountRowsPath, "verify MPT account rows"); err != nil {
		return common.Hash{}, err
	}
	return pbtVerifyHashMPT(accountRowsPath)
}

func pbtVerifyBuildAccountRows(accountsPath, storagePath string, output *etl.Collector) error {
	accountFile, accountReader, err := pbtVerifyOpenKV(accountsPath)
	if err != nil {
		return err
	}
	defer accountFile.Close()
	storageFile, storageReader, err := pbtVerifyOpenKV(storagePath)
	if err != nil {
		return err
	}
	defer storageFile.Close()
	storage := &pbtVerifyStorageIterator{reader: storageReader}
	accountProgress := pbtVerifyProgress{logger: log.Root(), msg: "PBT verify progress", phase: "MPT storage roots", next: time.Now()}
	accountKey, accountValue, accountOK, err := accountReader.next()
	if err != nil {
		return err
	}
	for accountOK {
		accountProgress.add(accountKey)
		if len(accountKey) != length.Hash {
			return fmt.Errorf("invalid account key")
		}
		var header artifact.Header
		if err := pbtVerifyDecode(accountValue, &header); err != nil {
			return fmt.Errorf("%w: %s: %w", errPBTVerifyScratchIO, accountsPath, err)
		}
		storage.prefix = accountKey
		root, err := trie.StreamHashIterator(storage, 40, trie.NewHashBuilder(false), false)
		if err != nil {
			return err
		}
		if storage.err != nil {
			return storage.err
		}
		account, err := pbtVerifyAccount(header, root, accountsPath)
		if err != nil {
			return err
		}
		encoded, err := pbtVerifyEncode(account)
		if err != nil {
			return err
		}
		if err := output.Collect(accountKey, encoded); err != nil {
			return err
		}
		accountKey, accountValue, accountOK, err = accountReader.next()
		if err != nil {
			return err
		}
	}
	if storage.hasKey {
		return fmt.Errorf("surplus storage row for %x", storage.key)
	}
	return nil
}

func pbtVerifyAccount(header artifact.Header, root common.Hash, path string) (pbtVerifyAccountRecord, error) {
	if len(header.Nonce) > 8 {
		return pbtVerifyAccountRecord{}, fmt.Errorf("%w: %s: header nonce has width %d", errPBTVerifyScratchIO, path, len(header.Nonce))
	}
	if len(header.CodeSize) > 4 {
		return pbtVerifyAccountRecord{}, fmt.Errorf("%w: %s: header code size has width %d", errPBTVerifyScratchIO, path, len(header.CodeSize))
	}
	account := accounts.Account{Nonce: dbstate.PBinIntegerUint64(header.Nonce), Root: root}
	account.Balance.SetBytes(header.Balance)
	switch header.Kind {
	case 0:
		account.CodeHash = accounts.EmptyCodeHash
	case 1:
		account.CodeHash = accounts.InternCodeHash(header.CodeHash)
	case 2:
		code := append(append([]byte{}, eip8297.DelegationMarker[:]...), header.Target[:]...)
		account.CodeHash = accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(code)))
	default:
		return pbtVerifyAccountRecord{}, fmt.Errorf("unknown account kind %d", header.Kind)
	}
	return pbtVerifyAccountRecord{Nonce: account.Nonce, Balance: account.Balance.Bytes(), Root: account.Root, CodeHash: account.CodeHash.Value()}, nil
}

type pbtVerifyStorageIterator struct {
	reader *pbtVerifyKVReader
	prefix []byte
	key    []byte
	value  []byte
	hasKey bool
	err    error
}

func (it *pbtVerifyStorageIterator) Next() (trie.StreamItem, []byte, *accounts.Account, []byte, []byte, []byte) {
	if !it.hasKey {
		it.key, it.value, it.hasKey, it.err = it.reader.next()
		if it.err != nil || !it.hasKey {
			return trie.NoItem, nil, nil, nil, nil, nil
		}
	}
	if len(it.key) != 2*length.Hash+length.Incarnation || !bytes.HasPrefix(it.key, it.prefix) {
		return trie.NoItem, nil, nil, nil, nil, nil
	}
	key := nibbles.KeybytesToHex(it.key)
	value := it.value
	it.hasKey = false
	return trie.StorageStreamItem, key[:len(key)-1], nil, nil, nil, value
}

func pbtVerifyHashMPT(path string) (common.Hash, error) {
	f, reader, err := pbtVerifyOpenKV(path)
	if err != nil {
		return common.Hash{}, err
	}
	defer f.Close()
	iterator := &pbtVerifyMPTIterator{reader: reader, progress: &pbtVerifyProgress{logger: log.Root(), msg: "PBT verify progress", phase: "MPT account trie", next: time.Now()}}
	root, err := trie.StreamHashIterator(iterator, 40, trie.NewHashBuilder(false), false)
	if err != nil {
		return common.Hash{}, err
	}
	if iterator.err != nil {
		return common.Hash{}, iterator.err
	}
	if root == (common.Hash{}) {
		return common.Hash{}, fmt.Errorf("empty MPT root")
	}
	return root, nil
}

type pbtVerifyMPTIterator struct {
	reader   *pbtVerifyKVReader
	progress *pbtVerifyProgress
	err      error
}

func (it *pbtVerifyMPTIterator) Next() (trie.StreamItem, []byte, *accounts.Account, []byte, []byte, []byte) {
	key, value, ok, err := it.reader.next()
	if err != nil {
		it.err = err
		return trie.NoItem, nil, nil, nil, nil, nil
	}
	if !ok {
		return trie.NoItem, nil, nil, nil, nil, nil
	}
	if len(key) == length.Hash {
		it.progress.add(key)
		var record pbtVerifyAccountRecord
		if err := pbtVerifyDecode(value, &record); err != nil {
			it.err = fmt.Errorf("%w: %s: %w", errPBTVerifyScratchIO, it.reader.path, err)
			return trie.NoItem, nil, nil, nil, nil, nil
		}
		account := &accounts.Account{Nonce: record.Nonce, Root: record.Root, CodeHash: accounts.InternCodeHash(record.CodeHash)}
		account.Balance.SetBytes(record.Balance)
		hexKey := nibbles.KeybytesToHex(key)
		return trie.AccountStreamItem, hexKey[:len(hexKey)-1], account, nil, nil, nil
	}
	return trie.NoItem, nil, nil, nil, nil, nil
}
