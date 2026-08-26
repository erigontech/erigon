package epbs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
)

type PendingPayloadStore interface {
	Save(context.Context, pendingPayloadKey, *pendingPayload, common.Bytes48) error
	Delete(context.Context, pendingPayloadKey) error
	Load(context.Context) ([]storedPendingPayload, error)
}

type storedPendingPayload struct {
	Slot               uint64
	ParentBlockHash    common.Hash
	ParentBlockRoot    common.Hash
	BlockHash          common.Hash
	BuilderIndex       uint64
	BuilderPubkey      common.Bytes48
	BidValue           uint64
	Parent             ParentInfo
	Version            clparams.StateVersion
	ExecutionPayload   []byte
	ExecutionRequests  []byte
	BlobKzgCommitments [][]byte
	BlobKzgProofs      [][]byte
	Blobs              [][]byte
}

type filePendingPayloadStore struct {
	dir       string
	beaconCfg *clparams.BeaconChainConfig
}

const (
	maxPendingPayloadFiles    = 256
	maxPendingPayloadFileSize = 128 << 20
)

func NewFilePendingPayloadStore(dir string, beaconCfg *clparams.BeaconChainConfig) PendingPayloadStore {
	return newFilePendingPayloadStore(dir, beaconCfg)
}

func newFilePendingPayloadStore(dir string, beaconCfg *clparams.BeaconChainConfig) *filePendingPayloadStore {
	return &filePendingPayloadStore{dir: dir, beaconCfg: beaconCfg}
}

func (s *filePendingPayloadStore) Save(ctx context.Context, key pendingPayloadKey, pending *pendingPayload, pubkey common.Bytes48) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record, err := encodeStoredPendingPayload(key, pending, pubkey)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".pending-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = dir.RemoveFile(tmpName) }()
	encoder := json.NewEncoder(tmp)
	if err := encoder.Encode(record); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path(key))
}

func (s *filePendingPayloadStore) Delete(ctx context.Context, key pendingPayloadKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := dir.RemoveFile(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *filePendingPayloadStore) Load(ctx context.Context) ([]storedPendingPayload, error) {
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	records := make([]storedPendingPayload, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		if len(records) >= maxPendingPayloadFiles {
			return nil, fmt.Errorf("too many persisted pending payloads")
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if info.Size() < 0 || info.Size() > maxPendingPayloadFileSize {
			return nil, fmt.Errorf("pending payload %s has invalid size %d", entry.Name(), info.Size())
		}
		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var record storedPendingPayload
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, fmt.Errorf("decode pending payload %s: %w", entry.Name(), err)
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *filePendingPayloadStore) path(key pendingPayloadKey) string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d-%x-%x.json", key.slot, key.parentBlockRoot, key.blockHash))
}

func encodeStoredPendingPayload(key pendingPayloadKey, pending *pendingPayload, pubkey common.Bytes48) (storedPendingPayload, error) {
	if pending == nil || pending.assembled == nil || pending.assembled.Eth1Block == nil {
		return storedPendingPayload{}, errors.New("pending payload is incomplete")
	}
	payload, err := pending.assembled.Eth1Block.EncodeSSZ(nil)
	if err != nil {
		return storedPendingPayload{}, err
	}
	var requests []byte
	if pending.execReqs != nil {
		requests, err = pending.execReqs.EncodeSSZ(nil)
		if err != nil {
			return storedPendingPayload{}, err
		}
	}
	record := storedPendingPayload{
		Slot: pending.slot, ParentBlockHash: key.parentBlockHash, ParentBlockRoot: key.parentBlockRoot,
		BlockHash: key.blockHash, BuilderIndex: pending.builderIndex, BuilderPubkey: pubkey,
		BidValue: pending.bidValue, Parent: pending.parent, Version: pending.assembled.Eth1Block.Version(),
		ExecutionPayload: payload, ExecutionRequests: requests,
	}
	if bundle := pending.assembled.BlobsBundle; bundle != nil {
		record.BlobKzgCommitments = bundle.Commitments
		record.BlobKzgProofs = bundle.Proofs
		record.Blobs = bundle.Blobs
	}
	return record, nil
}

func decodeStoredPendingPayload(record storedPendingPayload, beaconCfg *clparams.BeaconChainConfig) (pendingPayloadKey, *pendingPayload, error) {
	key := pendingPayloadKey{
		slot: record.Slot, parentBlockHash: record.ParentBlockHash,
		parentBlockRoot: record.ParentBlockRoot, blockHash: record.BlockHash,
	}
	if beaconCfg == nil {
		return key, nil, errors.New("beacon config is required")
	}
	if record.Version < clparams.GloasVersion || record.BidValue == 0 || len(record.ExecutionPayload) == 0 || len(record.ExecutionRequests) == 0 {
		return key, nil, errors.New("stored pending payload metadata is invalid")
	}
	if record.Parent.BlockRoot != record.ParentBlockRoot || record.Parent.ExecutionHash != record.ParentBlockHash {
		return key, nil, errors.New("stored parent identity mismatch")
	}
	payload := cltypes.NewEth1Block(record.Version, beaconCfg)
	if err := payload.DecodeSSZStrict(record.ExecutionPayload, int(record.Version)); err != nil {
		return key, nil, err
	}
	if payload.BlockHash != record.BlockHash || payload.ParentHash != record.ParentBlockHash || payload.SlotNumber != record.Slot {
		return key, nil, errors.New("stored execution payload identity mismatch")
	}
	requests := cltypes.NewExecutionRequestsWithVersion(beaconCfg, record.Version)
	if err := requests.DecodeSSZStrict(record.ExecutionRequests, int(record.Version)); err != nil {
		return key, nil, err
	}
	var bundle *eladapter.BlobsBundle
	if len(record.Blobs) > 0 || len(record.BlobKzgCommitments) > 0 || len(record.BlobKzgProofs) > 0 {
		bundle = &eladapter.BlobsBundle{
			Commitments: record.BlobKzgCommitments,
			Proofs:      record.BlobKzgProofs,
			Blobs:       record.Blobs,
		}
		maxBlobs := beaconCfg.GetBlobParameters(record.Slot / beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock
		if err := validateBlobsBundle(bundle, maxBlobs); err != nil {
			return key, nil, err
		}
	}
	return key, &pendingPayload{
		slot: record.Slot, builderIndex: record.BuilderIndex,
		assembled: &eladapter.AssembledPayload{Eth1Block: payload, BlobsBundle: bundle},
		execReqs:  requests, parent: record.Parent, bidValue: record.BidValue,
	}, nil
}
