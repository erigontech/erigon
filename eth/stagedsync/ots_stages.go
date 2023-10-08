package stagedsync

import (
	"context"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

// Standard Otterscan V2 stages; if opted-in, they must be inserted before finish stage.
func OtsStages(ctx context.Context, caCfg ContractAnalyzerCfg) []*Stage {
	return []*Stage{
		{
			ID:          stages.OtsContractIndexer,
			Description: "Index contract creation",
			Forward:     GenericStageForwardFunc(ctx, caCfg, stages.Bodies, ContractIndexerExecutor),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsAllContracts,
					kv.OtsAllContractsCounter,
					nil,
				),
			),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC20Indexer,
			Description: "ERC20 token indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsContractIndexer,
				NewConcurrentIndexerExecutor(
					NewERC20Prober,
					kv.OtsAllContracts,
					kv.OtsERC20,
					kv.OtsERC20Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC20,
					kv.OtsERC20Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC20),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC165Indexer,
			Description: "ERC165 indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsContractIndexer,
				NewConcurrentIndexerExecutor(
					NewERC165Prober,
					kv.OtsAllContracts,
					kv.OtsERC165,
					kv.OtsERC165Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC165,
					kv.OtsERC165Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC165),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC721Indexer,
			Description: "ERC721 token indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsERC165Indexer,
				NewConcurrentIndexerExecutor(
					NewERC721Prober,
					kv.OtsERC165,
					kv.OtsERC721,
					kv.OtsERC721Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC721,
					kv.OtsERC721Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC721, kv.ADDR_ATTR_ERC721_MD),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC1155Indexer,
			Description: "ERC1155 token indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsERC165Indexer,
				NewConcurrentIndexerExecutor(
					NewERC1155Prober,
					kv.OtsERC165,
					kv.OtsERC1155,
					kv.OtsERC1155Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC1155,
					kv.OtsERC1155Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC1155),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC1167Indexer,
			Description: "ERC1167 proxy indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsContractIndexer,
				NewConcurrentIndexerExecutor(
					NewERC1167Prober,
					kv.OtsAllContracts,
					kv.OtsERC1167,
					kv.OtsERC1167Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC1167,
					kv.OtsERC1167Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC1167),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC4626Indexer,
			Description: "ERC4626 token indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsERC20Indexer,
				NewConcurrentIndexerExecutor(
					NewERC4626Prober,
					kv.OtsERC20,
					kv.OtsERC4626,
					kv.OtsERC4626Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC4626,
					kv.OtsERC4626Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC4626),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC20And721Transfers,
			Description: "ERC20/721 token transfer indexer",
			// Binds itself to ERC721 contract classifier as the parent stage on purpose to ensure
			// both ERC20 and ERC721 stages are executed.
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsERC721Indexer, ERC20And721TransferIndexerExecutor),
			Unwind:  GenericStageUnwindFunc(ctx, caCfg, NewGenericLogIndexerUnwinder()),
			Prune:   NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC20And721Holdings,
			Description: "ERC20/721 token holdings indexer",
			Forward:     GenericStageForwardFunc(ctx, caCfg, stages.OtsERC721Indexer, ERC20And721HolderIndexerExecutor),
			Unwind:      GenericStageUnwindFunc(ctx, caCfg, NewGenericLogHoldingsUnwinder()),
			Prune:       NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsWithdrawals,
			Description: "CL withdrawals indexer",
			Forward:     GenericStageForwardFunc(ctx, caCfg, stages.Bodies, WithdrawalsExecutor),
			Unwind:      GenericStageUnwindFunc(ctx, caCfg, NewGenericBlockIndexerUnwinder()),
			Prune:       NoopStagePrune(ctx, caCfg),
		},
	}
}
