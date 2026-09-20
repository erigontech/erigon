// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

const (
	pendingPayloadFormatVersion = 1
	maxPendingPayloadBytes      = 64 << 20
)

type PendingPayloadStore struct {
	mu            sync.Mutex
	directory     string
	beaconCfg     *clparams.BeaconChainConfig
	maxRetained   int
	syncDirectory func(string) error
}

type pendingPayloadRecord struct {
	Version       uint32
	Identity      PayloadIdentity
	PayloadSSZ    []byte
	Requests      [][]byte
	Blobs         *eladapter.BlobsBundle
	BlockValue    []byte
	BidValue      uint64
	BuilderIndex  uint64
	BuilderPubkey common.Bytes48
	SignedBidRoot common.Hash
	GenesisRoot   common.Hash
	Checksum      common.Hash
}

func OpenPendingPayloadStore(directory string, beaconCfg *clparams.BeaconChainConfig, maxRetained int) (*PendingPayloadStore, error) {
	if directory == "" || beaconCfg == nil || beaconCfg.SlotsPerEpoch == 0 || maxRetained <= 0 {
		return nil, errors.New("epbs/pending store: invalid configuration")
	}
	var created []string
	for path := filepath.Clean(directory); ; path = filepath.Dir(path) {
		_, err := os.Stat(path)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) || filepath.Dir(path) == path {
			return nil, fmt.Errorf("epbs/pending store: inspect parent directory: %w", err)
		}
		created = append(created, path)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("epbs/pending store: create directory: %w", err)
	}
	for _, createdPath := range slices.Backward(created) {
		if err := dir.FsyncDir(filepath.Dir(createdPath)); err != nil {
			return nil, fmt.Errorf("epbs/pending store: sync created directory: %w", err)
		}
	}
	info, err := os.Stat(directory)
	if err != nil {
		return nil, fmt.Errorf("epbs/pending store: inspect directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("epbs/pending store: directory must be private")
	}
	return &PendingPayloadStore{directory: directory, beaconCfg: beaconCfg, maxRetained: maxRetained, syncDirectory: dir.FsyncDir}, nil
}

func (s *PendingPayloadStore) Save(identity PayloadIdentity, payload *RetainedPayload) error {
	if s == nil {
		return errors.New("epbs/pending store: unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.makeRecord(identity, payload)
	if err != nil {
		return err
	}
	encoded, err := marshalPendingPayloadRecord(record)
	if err != nil {
		return err
	}
	if len(encoded) > maxPendingPayloadBytes {
		return errors.New("epbs/pending store: payload exceeds storage limit")
	}
	file, err := os.CreateTemp(s.directory, ".pending-*")
	if err != nil {
		return fmt.Errorf("epbs/pending store: create temporary record: %w", err)
	}
	defer func() { _ = dir.RemoveFile(file.Name()) }()
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return fmt.Errorf("epbs/pending store: protect record: %w", err)
	}
	if _, err := file.Write(encoded); err != nil {
		file.Close()
		return fmt.Errorf("epbs/pending store: write record: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("epbs/pending store: sync record: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("epbs/pending store: close record: %w", err)
	}
	if err := os.Link(file.Name(), s.recordPath(identity)); err != nil {
		return fmt.Errorf("epbs/pending store: publish record: %w", err)
	}
	if err := dir.RemoveFile(file.Name()); err != nil {
		return errors.Join(fmt.Errorf("epbs/pending store: remove temporary record: %w", err), s.removeRecord(identity))
	}
	if err := s.syncDirectory(s.directory); err != nil {
		return errors.Join(fmt.Errorf("epbs/pending store: sync published record: %w", err), s.removeRecord(identity))
	}
	return nil
}

func (s *PendingPayloadStore) Load(currentSlot uint64) (map[PayloadIdentity]*RetainedPayload, error) {
	if s == nil {
		return nil, errors.New("epbs/pending store: unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, fmt.Errorf("epbs/pending store: list records: %w", err)
	}
	loaded := make(map[PayloadIdentity]*RetainedPayload)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".pending-") {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".json") || !entry.Type().IsRegular() {
			return nil, fmt.Errorf("epbs/pending store: unexpected entry %q", entry.Name())
		}
		path := filepath.Join(s.directory, entry.Name())
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("epbs/pending store: open record: %w", err)
		}
		encoded, readErr := io.ReadAll(io.LimitReader(file, maxPendingPayloadBytes+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return nil, fmt.Errorf("epbs/pending store: read record: %w", errors.Join(readErr, closeErr))
		}
		if len(encoded) > maxPendingPayloadBytes {
			return nil, errors.New("epbs/pending store: record exceeds storage limit")
		}
		record, err := unmarshalPendingPayloadRecord(encoded)
		if err != nil {
			return nil, fmt.Errorf("epbs/pending store: decode %q: %w", entry.Name(), err)
		}
		if entry.Name() != s.recordName(record.Identity) {
			return nil, fmt.Errorf("epbs/pending store: identity mismatch in %q", entry.Name())
		}
		if record.Identity.Slot < currentSlot {
			continue
		}
		if len(loaded) >= s.maxRetained {
			return nil, errors.New("epbs/pending store: retained capacity exceeded")
		}
		payload, err := s.decodeRecord(record)
		if err != nil {
			return nil, fmt.Errorf("epbs/pending store: validate %q: %w", entry.Name(), err)
		}
		if _, exists := loaded[record.Identity]; exists {
			return nil, errors.New("epbs/pending store: duplicate payload identity")
		}
		loaded[record.Identity] = payload
	}
	return loaded, nil
}

func (s *PendingPayloadStore) Delete(identity PayloadIdentity) error {
	if s == nil {
		return errors.New("epbs/pending store: unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removeRecord(identity)
}

func (s *PendingPayloadStore) removeRecord(identity PayloadIdentity) error {
	if err := dir.RemoveFile(s.recordPath(identity)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("epbs/pending store: remove record: %w", err)
	}
	return s.syncDirectory(s.directory)
}

func (s *PendingPayloadStore) PruneBeforeSlot(slot uint64) error {
	if s == nil {
		return errors.New("epbs/pending store: unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return fmt.Errorf("epbs/pending store: list records: %w", err)
	}
	changed := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".pending-") {
			if err := dir.RemoveFile(filepath.Join(s.directory, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("epbs/pending store: remove incomplete record: %w", err)
			}
			changed = true
			continue
		}
		var recordSlot uint64
		if _, err := fmt.Sscanf(entry.Name(), "%d-", &recordSlot); err != nil {
			return fmt.Errorf("epbs/pending store: unexpected record %q: %w", entry.Name(), err)
		}
		if recordSlot < slot {
			if err := dir.RemoveFile(filepath.Join(s.directory, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("epbs/pending store: prune record: %w", err)
			}
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return s.syncDirectory(s.directory)
}

func (s *PendingPayloadStore) makeRecord(identity PayloadIdentity, payload *RetainedPayload) (pendingPayloadRecord, error) {
	owned, err := cloneRetainedPayload(s.beaconCfg, payload)
	if err != nil {
		return pendingPayloadRecord{}, fmt.Errorf("epbs/pending store: clone payload: %w", err)
	}
	if owned.Assembled.Eth1Block.SlotNumber != identity.Slot || owned.Assembled.Eth1Block.ParentHash != identity.ParentBlockHash ||
		owned.Assembled.Eth1Block.BlockHash != identity.BlockHash || identity.ParentBlockRoot == (common.Hash{}) ||
		owned.SignedBidRoot == (common.Hash{}) || owned.BuilderPubkey == (common.Bytes48{}) || owned.Assembled.BlockValue.Sign() < 0 {
		return pendingPayloadRecord{}, errors.New("epbs/pending store: invalid payload identity")
	}
	encodedPayload, err := owned.Assembled.Eth1Block.EncodeSSZ(nil)
	if err != nil {
		return pendingPayloadRecord{}, fmt.Errorf("epbs/pending store: encode payload: %w", err)
	}
	if _, _, err := decodeExecutionRequests(s.beaconCfg, owned.Assembled.RequestsBundle); err != nil {
		return pendingPayloadRecord{}, fmt.Errorf("epbs/pending store: requests: %w", err)
	}
	if _, err := buildBlobCommitments(s.beaconCfg, identity.Slot, owned.Assembled.BlobsBundle); err != nil {
		return pendingPayloadRecord{}, fmt.Errorf("epbs/pending store: blobs: %w", err)
	}
	return pendingPayloadRecord{
		Version: pendingPayloadFormatVersion, Identity: identity, PayloadSSZ: encodedPayload,
		Requests: owned.Assembled.RequestsBundle.Requests, Blobs: owned.Assembled.BlobsBundle,
		BlockValue: owned.Assembled.BlockValue.Bytes(), BidValue: owned.BidValue,
		BuilderIndex: owned.BuilderIndex, BuilderPubkey: owned.BuilderPubkey,
		SignedBidRoot: owned.SignedBidRoot, GenesisRoot: owned.GenesisRoot,
	}, nil
}

func (s *PendingPayloadStore) decodeRecord(record pendingPayloadRecord) (*RetainedPayload, error) {
	if record.Version != pendingPayloadFormatVersion || len(record.PayloadSSZ) == 0 {
		return nil, errors.New("unsupported or empty pending payload record")
	}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, s.beaconCfg)
	if err := payload.DecodeSSZStrict(record.PayloadSSZ, int(clparams.GloasVersion)); err != nil {
		return nil, err
	}
	reencoded, err := payload.EncodeSSZ(nil)
	if err != nil || !bytes.Equal(reencoded, record.PayloadSSZ) {
		return nil, errors.New("non-canonical execution payload")
	}
	requests := &typesproto.RequestsBundle{Requests: record.Requests}
	executionRequests, _, err := decodeExecutionRequests(s.beaconCfg, requests)
	if err != nil {
		return nil, err
	}
	assembled := &eladapter.AssembledPayload{
		Eth1Block: payload, RequestsBundle: requests, BlobsBundle: record.Blobs,
		BlockValue: new(big.Int).SetBytes(record.BlockValue),
	}
	retained := &RetainedPayload{
		Assembled: assembled, ExecutionRequests: executionRequests, BidValue: record.BidValue,
		BuilderIndex: record.BuilderIndex, BuilderPubkey: record.BuilderPubkey,
		SignedBidRoot: record.SignedBidRoot, GenesisRoot: record.GenesisRoot,
	}
	if _, err := s.makeRecord(record.Identity, retained); err != nil {
		return nil, err
	}
	return retained, nil
}

func marshalPendingPayloadRecord(record pendingPayloadRecord) ([]byte, error) {
	record.Checksum = common.Hash{}
	content, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	record.Checksum = common.Hash(sha256.Sum256(content))
	return json.Marshal(record)
}

func unmarshalPendingPayloadRecord(encoded []byte) (pendingPayloadRecord, error) {
	var record pendingPayloadRecord
	if err := json.Unmarshal(encoded, &record); err != nil {
		return record, err
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(encoded, canonical) {
		return record, errors.New("non-canonical pending payload record")
	}
	want := record.Checksum
	record.Checksum = common.Hash{}
	content, err := json.Marshal(record)
	if err != nil || common.Hash(sha256.Sum256(content)) != want {
		return record, errors.New("pending payload checksum mismatch")
	}
	record.Checksum = want
	return record, nil
}

func (s *PendingPayloadStore) recordName(identity PayloadIdentity) string {
	return fmt.Sprintf("%020d-%x-%x-%x.json", identity.Slot, identity.ParentBlockRoot, identity.ParentBlockHash, identity.BlockHash)
}

func (s *PendingPayloadStore) recordPath(identity PayloadIdentity) string {
	return filepath.Join(s.directory, s.recordName(identity))
}
