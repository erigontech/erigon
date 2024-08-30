package main

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/datastream/client"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/debug_tools"
)

func main() {
	ctx := context.Background()
	cfg, err := debug_tools.GetConf()
	if err != nil {
		panic(fmt.Sprintf("RPGCOnfig: %s", err))
	}

	// Create client
	client := client.NewClient(ctx, cfg.Datastream, 3, 500, 0)

	// Start client (connect to the server)
	defer client.Stop()
	if err := client.Start(); err != nil {
		panic(err)
	}

	// create bookmark
	bookmark := types.NewBookmarkProto(0, datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)

	var previousFile *types.FileEntry
	var lastBlockRoot common.Hash
	progressBatch := uint64(0)
	progressBlock := uint64(0)
	lastSeenBatch := uint64(0)
	lastSeenBlock := uint64(0)

	function := func(file *types.FileEntry) error {
		switch file.EntryType {
		case types.EntryTypeL2BlockEnd:
			if previousFile != nil && previousFile.EntryType != types.EntryTypeL2Block && previousFile.EntryType != types.EntryTypeL2Tx {
				return fmt.Errorf("unexpected entry type before l2 block end: %v", previousFile.EntryType)
			}
		case types.BookmarkEntryType:
			bookmark, err := types.UnmarshalBookmark(file.Data)
			if err != nil {
				return err
			}
			if bookmark.BookmarkType() == datastream.BookmarkType_BOOKMARK_TYPE_BATCH {
				progressBatch = bookmark.Value
				if previousFile != nil && previousFile.EntryType != types.EntryTypeBatchEnd {
					return fmt.Errorf("unexpected entry type before batch bookmark type: %v, bookmark batch number: %d", previousFile.EntryType, bookmark.Value)
				}
			}
			if bookmark.BookmarkType() == datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK {
				progressBlock = bookmark.Value
				if previousFile != nil &&
					previousFile.EntryType != types.EntryTypeBatchStart &&
					previousFile.EntryType != types.EntryTypeL2BlockEnd {
					return fmt.Errorf("unexpected entry type before block bookmark type: %v, bookmark block number: %d", previousFile.EntryType, bookmark.Value)
				}
			}
		case types.EntryTypeBatchStart:
			batchStart, err := types.UnmarshalBatchStart(file.Data)
			if err != nil {
				return err
			}
			if lastSeenBatch+1 != batchStart.Number {
				return fmt.Errorf("unexpected batch %d, expected %d", batchStart.Number, lastSeenBatch+1)
			}
			lastSeenBatch = batchStart.Number
			progressBatch = batchStart.Number
			if previousFile != nil {
				if previousFile.EntryType != types.BookmarkEntryType {
					return fmt.Errorf("unexpected entry type before batch start: %v, batchStart Batch number: %d", previousFile.EntryType, batchStart.Number)
				} else {
					bookmark, err := types.UnmarshalBookmark(previousFile.Data)
					if err != nil {
						return err
					}
					if bookmark.BookmarkType() != datastream.BookmarkType_BOOKMARK_TYPE_BATCH {
						return fmt.Errorf("unexpected bookmark type before batch start: %v, batchStart Batch number: %d", bookmark.BookmarkType(), batchStart.Number)
					}
				}
			}
		case types.EntryTypeBatchEnd:
			if previousFile != nil &&
				previousFile.EntryType != types.EntryTypeL2BlockEnd &&
				previousFile.EntryType != types.EntryTypeL2Tx &&
				previousFile.EntryType != types.EntryTypeL2Block &&
				previousFile.EntryType != types.EntryTypeBatchStart {
				return fmt.Errorf("unexpected entry type before batch end: %v", previousFile.EntryType)
			}
			batchEnd, err := types.UnmarshalBatchEnd(file.Data)
			if err != nil {
				return err
			}
			if batchEnd.Number != progressBatch {
				return fmt.Errorf("batch end number mismatch: %d, expected: %d", batchEnd.Number, progressBatch)
			}
			if batchEnd.StateRoot != lastBlockRoot {
				return fmt.Errorf("batch end state root mismatch: %x, expected: %x", batchEnd.StateRoot, lastBlockRoot)
			}
		case types.EntryTypeL2Tx:
			if previousFile != nil && previousFile.EntryType != types.EntryTypeL2Tx && previousFile.EntryType != types.EntryTypeL2Block {
				return fmt.Errorf("unexpected entry type before l2 tx: %v", previousFile.EntryType)
			}
		case types.EntryTypeL2Block:
			l2Block, err := types.UnmarshalL2Block(file.Data)
			if err != nil {
				return err
			}
			if l2Block.L2BlockNumber > 0 && lastSeenBlock+1 != l2Block.L2BlockNumber {
				return fmt.Errorf("unexpected block %d, expected %d", l2Block.L2BlockNumber, lastSeenBlock+1)
			}
			lastSeenBlock = l2Block.L2BlockNumber
			progressBlock = l2Block.L2BlockNumber
			if previousFile != nil {
				if previousFile.EntryType != types.BookmarkEntryType && !previousFile.IsL2BlockEnd() {
					return fmt.Errorf("unexpected entry type before l2 block: %v, block number: %d", previousFile.EntryType, l2Block.L2BlockNumber)
				} else {
					bookmark, err := types.UnmarshalBookmark(previousFile.Data)
					if err != nil {
						return err
					}
					if bookmark.BookmarkType() != datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK {
						return fmt.Errorf("unexpected bookmark type before l2 block: %v, block number: %d", bookmark.BookmarkType(), l2Block.L2BlockNumber)
					}

				}
			}
			lastBlockRoot = l2Block.StateRoot
		case types.EntryTypeGerUpdate:
			return nil
		default:
			return fmt.Errorf("unexpected entry type: %v", file.EntryType)
		}

		previousFile = file
		return nil
	}

	// send start command
	err = client.ExecutePerFile(bookmark, function)
	fmt.Println("progress block: ", progressBlock)
	fmt.Println("progress batch: ", progressBatch)
	if err != nil {
		panic(err)
	}
}
