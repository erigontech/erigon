package persistence

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/spf13/afero"
)

type beaconChainDatabaseFilesystem struct {
	fs  afero.Fs
	cfg *clparams.BeaconChainConfig

	fullBlocks bool // same encoding as reqresp

	executionEngine execution_client.ExecutionEngine
	indiciesDB      *sql.DB
}

func NewBeaconChainDatabaseFilesystem(fs afero.Fs, executionEngine execution_client.ExecutionEngine, fullBlocks bool, cfg *clparams.BeaconChainConfig, indiciesDB *sql.DB) BeaconChainDatabase {
	return beaconChainDatabaseFilesystem{
		fs:              fs,
		cfg:             cfg,
		fullBlocks:      fullBlocks,
		indiciesDB:      indiciesDB,
		executionEngine: executionEngine,
	}
}

func (b beaconChainDatabaseFilesystem) GetRange(ctx context.Context, from uint64, count uint64) ([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], error) {
	tx, err := b.indiciesDB.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Retrieve block roots for each ranged slot
	beaconBlockRooots, slots, err := beacon_indicies.ReadBeaconBlockRootsInSlotRange(ctx, tx, from, count)
	if err != nil {
		return nil, err
	}

	if len(beaconBlockRooots) == 0 {
		return nil, nil
	}
	var startELNumber *uint64
	var firstPostBellatrixBlock *int

	elBlockCount := uint64(0)
	blocks := []*peers.PeeredObject[*cltypes.SignedBeaconBlock]{}
	for idx, blockRoot := range beaconBlockRooots {
		slot := slots[idx]
		_, path := RootToPaths(blockRoot, b.cfg)

		fp, err := b.fs.OpenFile(path, os.O_RDONLY, 0o755)
		if err != nil {
			return nil, err
		}
		defer fp.Close()
		block := cltypes.NewSignedBeaconBlock(b.cfg)
		version := b.cfg.GetCurrentStateVersion(slot / b.cfg.SlotsPerEpoch)
		if b.fullBlocks {
			if err := ssz_snappy.DecodeAndReadNoForkDigest(fp, block, version); err != nil {
				return nil, err
			}
		} else {
			// Below is a frankenstein monster, i am sorry.
			executionPayloadHeader := cltypes.NewEth1Header(version)
			if version >= clparams.BellatrixVersion {
				elBlockCount++

				// If there is no execution engine, abort.
				if b.executionEngine == nil {
					return nil, nil
				}
				executionPayloadLengthBytes := make([]byte, 8)
				if _, err := fp.Read(executionPayloadLengthBytes); err != nil {
					return nil, err
				}

				executionPayloadLength := binary.BigEndian.Uint64(executionPayloadLengthBytes)

				executionPayloadBytes := make([]byte, executionPayloadLength)
				if _, err := fp.Read(executionPayloadBytes); err != nil {
					return nil, err
				}
				if err := executionPayloadHeader.DecodeSSZ(executionPayloadBytes, int(version)); err != nil {
					return nil, err
				}
				if startELNumber == nil {
					startELNumber = new(uint64)
					*startELNumber = executionPayloadHeader.BlockNumber
					firstPostBellatrixBlock = new(int)
					*firstPostBellatrixBlock = len(blocks)
				}
			}
			// Read beacon part of the block
			beaconBlockLengthBytes := make([]byte, 8)
			if _, err := fp.Read(beaconBlockLengthBytes); err != nil {
				return nil, err
			}
			beaconBlockLength := binary.BigEndian.Uint64(beaconBlockLengthBytes)

			beaconBlockBytes := make([]byte, beaconBlockLength)
			if _, err := fp.Read(beaconBlockBytes); err != nil {
				return nil, err
			}
			if beaconBlockBytes, err = utils.DecompressSnappy(beaconBlockBytes); err != nil {
				return nil, err
			}

			if err := block.DecodeForStorage(beaconBlockBytes, int(version)); err != nil {
				return nil, err
			}
			// Write execution payload except for body part (withdrawals and transactions)
			if version >= clparams.BellatrixVersion {
				block.Block.Body.ExecutionPayload = cltypes.NewEth1Block(block.Version(), b.cfg)
				block.Block.Body.ExecutionPayload.ParentHash = executionPayloadHeader.ParentHash
				block.Block.Body.ExecutionPayload.FeeRecipient = executionPayloadHeader.FeeRecipient
				block.Block.Body.ExecutionPayload.StateRoot = executionPayloadHeader.StateRoot
				block.Block.Body.ExecutionPayload.ReceiptsRoot = executionPayloadHeader.ReceiptsRoot
				block.Block.Body.ExecutionPayload.LogsBloom = executionPayloadHeader.LogsBloom
				block.Block.Body.ExecutionPayload.PrevRandao = executionPayloadHeader.PrevRandao
				block.Block.Body.ExecutionPayload.BlockNumber = executionPayloadHeader.BlockNumber
				block.Block.Body.ExecutionPayload.GasLimit = executionPayloadHeader.GasLimit
				block.Block.Body.ExecutionPayload.GasUsed = executionPayloadHeader.GasUsed
				block.Block.Body.ExecutionPayload.Time = executionPayloadHeader.Time
				block.Block.Body.ExecutionPayload.Extra = executionPayloadHeader.Extra
				block.Block.Body.ExecutionPayload.BaseFeePerGas = executionPayloadHeader.BaseFeePerGas
				block.Block.Body.ExecutionPayload.BlockHash = executionPayloadHeader.BlockHash
				block.Block.Body.ExecutionPayload.BlobGasUsed = executionPayloadHeader.BlobGasUsed
				block.Block.Body.ExecutionPayload.ExcessBlobGas = executionPayloadHeader.ExcessBlobGas
			}
		}
		blocks = append(blocks, &peers.PeeredObject[*cltypes.SignedBeaconBlock]{Data: block})
	}
	if startELNumber != nil {
		bodies, err := b.executionEngine.GetBodiesByRange(*startELNumber, count)
		if err != nil {
			return nil, err
		}
		if len(bodies) != int(elBlockCount) {
			return nil, nil
		}

		for beaconBlockIdx, bodyIdx := *firstPostBellatrixBlock, 0; beaconBlockIdx < len(blocks); beaconBlockIdx, bodyIdx = beaconBlockIdx+1, bodyIdx+1 {
			body := bodies[bodyIdx]
			blocks[beaconBlockIdx].Data.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions(bodies[bodyIdx].Transactions)
			blocks[beaconBlockIdx].Data.Block.Body.ExecutionPayload.Withdrawals = solid.NewDynamicListSSZFromList[*types.Withdrawal](body.Withdrawals, int(b.cfg.MaxWithdrawalsPerPayload))
		}
	}
	return blocks, nil

}

func (b beaconChainDatabaseFilesystem) PurgeRange(ctx context.Context, from uint64, count uint64) error {
	tx, err := b.indiciesDB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := beacon_indicies.IterateBeaconIndicies(ctx, tx, from, from+count, func(_ uint64, beaconBlockRoot, _, _ libcommon.Hash, _ bool) bool {
		_, path := RootToPaths(beaconBlockRoot, b.cfg)
		_ = b.fs.Remove(path)
		return true
	}); err != nil {
		return err
	}

	if err := beacon_indicies.PruneIndicies(ctx, tx, from, from+count); err != nil {
		return err
	}

	return tx.Commit()
}

func (b beaconChainDatabaseFilesystem) WriteBlock(ctx context.Context, block *cltypes.SignedBeaconBlock, canonical bool) error {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	folderPath, path := RootToPaths(blockRoot, b.cfg)
	// ignore this error... reason: windows
	_ = b.fs.MkdirAll(folderPath, 0o755)
	fp, err := b.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	defer fp.Close()
	err = fp.Truncate(0)
	if err != nil {
		return err
	}
	_, err = fp.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	if b.fullBlocks { // 10x bigger but less latency
		err = ssz_snappy.EncodeAndWrite(fp, block)
		if err != nil {
			return err
		}
	} else {
		if block.Version() >= clparams.BellatrixVersion {
			// Keep track of EL headers so that we can do deduplication.
			payloadHeader, err := block.Block.Body.ExecutionPayload.PayloadHeader()
			if err != nil {
				return err
			}
			encodedPayloadHeader, err := payloadHeader.EncodeSSZ(nil)
			if err != nil {
				return err
			}

			// Need to reference EL somehow on read.
			if _, err := fp.Write(dbutils.EncodeBlockNumber(uint64(len(encodedPayloadHeader)))); err != nil {
				return err
			}
			if _, err := fp.Write(encodedPayloadHeader); err != nil {
				return err
			}
		}
		encoded, err := block.EncodeForStorage(nil)
		if err != nil {
			return err
		}
		compressedEncoded := utils.CompressSnappy(encoded)
		if _, err := fp.Write(dbutils.EncodeBlockNumber(uint64(len(compressedEncoded)))); err != nil {
			return err
		}
		if _, err := fp.Write(compressedEncoded); err != nil {
			return err
		}
	}

	err = fp.Sync()
	if err != nil {
		return err
	}

	tx, err := b.indiciesDB.Begin()
	if err != nil {
		return err
	}

	if err := beacon_indicies.GenerateBlockIndicies(ctx, tx, block, canonical); err != nil {
		return err
	}
	return tx.Commit()
}

// SlotToPaths define the file structure to store a block
//
// superEpoch = floor(slot / (epochSize ^ 2))
// epoch =  floot(slot / epochSize)
// file is to be stored at
// "/signedBeaconBlock/{superEpoch}/{epoch}/{slot}.ssz_snappy"
func RootToPaths(root libcommon.Hash, config *clparams.BeaconChainConfig) (folderPath string, filePath string) {
	folderPath = path.Clean(fmt.Sprintf("%02x/%02x", root[0], root[1]))
	filePath = path.Clean(fmt.Sprintf("%s/%x.sz", folderPath, root))
	return
}

func ValidateEpoch(fs afero.Fs, epoch uint64, config *clparams.BeaconChainConfig) error {
	superEpoch := epoch / (config.SlotsPerEpoch)

	// the folder path is superEpoch/epoch
	folderPath := path.Clean(fmt.Sprintf("%d/%d", superEpoch, epoch))

	fi, err := afero.ReadDir(fs, folderPath)
	if err != nil {
		return err
	}
	for _, fn := range fi {
		fn.Name()
	}
	return nil
}
