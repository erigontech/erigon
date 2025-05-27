package silkworm

import (
	"errors"
	"math"
	"time"
	"unsafe"

	silkworm_go "github.com/erigontech/silkworm-go"

	coresnaptype "github.com/erigontech/erigon-db/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

type SnapshotsRepository struct {
	silkworm *Silkworm

	blockSnapshots *freezeblocks.RoSnapshots
	stateSnapshots *state.Aggregator

	logger log.Logger
}

func NewSnapshotsRepository(
	silkworm *Silkworm,
	blockSnapshots *freezeblocks.RoSnapshots,
	stateSnapshots *state.Aggregator,
	logger log.Logger,
) *SnapshotsRepository {
	return &SnapshotsRepository{
		silkworm,
		blockSnapshots,
		stateSnapshots,
		logger,
	}
}

type MemoryMappedFile interface {
	FilePath() string
	DataHandle() unsafe.Pointer
	Size() int64
}

var _ MemoryMappedFile = (*seg.Decompressor)(nil)
var _ MemoryMappedFile = (*recsplit.Index)(nil)
var _ MemoryMappedFile = (*state.BtIndex)(nil)

func memoryMappedFile(file MemoryMappedFile) silkworm_go.MemoryMappedFile {
	return silkworm_go.MemoryMappedFile{
		FilePath:   NewFilePath(file.FilePath()),
		DataHandle: file.DataHandle(),
		Size:       file.Size(),
	}
}

func (r *SnapshotsRepository) Update() error {
	startTime := time.Now()
	r.logger.Debug("[silkworm] snapshots updating...")

	blocksView := r.blockSnapshots.View()
	defer blocksView.Close()
	err := r.updateBlocks(blocksView)
	if err != nil {
		return err
	}

	stateTx := r.stateSnapshots.BeginFilesRo()
	defer stateTx.Close()
	err = r.updateState(stateTx)

	if err == nil {
		duration := time.Since(startTime)
		if duration > 10*time.Second {
			r.logger.Info("[silkworm] snapshots updated", "duration", duration)
		} else {
			r.logger.Debug("[silkworm] snapshots updated", "duration", duration)
		}
	}
	return err
}

func (r *SnapshotsRepository) updateBlocks(view *freezeblocks.View) error {
	segmentsHeaders := view.Headers()
	segmentsBodies := view.Bodies()
	segmentsTransactions := view.Txs()

	count := len(segmentsHeaders)
	if (len(segmentsBodies) != count) || (len(segmentsTransactions) != count) {
		return errors.New("silkworm.SnapshotsRepository.updateBlocks: the number of headers/bodies/transactions segments must be the same")
	}

	startTime := time.Now()
	for i := 0; i < count; i++ {
		r.logger.Trace("[silkworm] snapshots updating blocks", "i", i, "count", count)
		segmentHeaders := segmentsHeaders[i].Src()
		segmentBodies := segmentsBodies[i].Src()
		segmentTransactions := segmentsTransactions[i].Src()

		err := r.silkworm.AddBlocksSnapshotBundle(BlocksSnapshotBundle{
			Headers: HeadersSnapshot{
				Segment:         memoryMappedFile(segmentHeaders),
				HeaderHashIndex: memoryMappedFile(segmentHeaders.Index()),
			},
			Bodies: BodiesSnapshot{
				Segment:       memoryMappedFile(segmentBodies),
				BlockNumIndex: memoryMappedFile(segmentBodies.Index()),
			},
			Transactions: TransactionsSnapshot{
				Segment:            memoryMappedFile(segmentTransactions),
				TxnHashIndex:       memoryMappedFile(segmentTransactions.Index(coresnaptype.Indexes.TxnHash)),
				TxnHash2BlockIndex: memoryMappedFile(segmentTransactions.Index(coresnaptype.Indexes.TxnHash2BlockNum)),
			},
		})
		if err != nil {
			return err
		}
	}
	r.logger.Debug("[silkworm] snapshots updated blocks", "count", count, "duration", time.Since(startTime))

	return nil
}

func makeInvertedIndexSnapshot(item state.FilesItem) InvertedIndexSnapshot {
	return InvertedIndexSnapshot{
		Segment:       memoryMappedFile(item.Segment()),
		AccessorIndex: memoryMappedFile(item.AccessorIndex()),
	}
}

func makeHistorySnapshot(historyItem state.FilesItem, iiItem state.FilesItem) HistorySnapshot {
	return HistorySnapshot{
		Segment:       memoryMappedFile(historyItem.Segment()),
		AccessorIndex: memoryMappedFile(historyItem.AccessorIndex()),
		InvertedIndex: makeInvertedIndexSnapshot(iiItem),
	}
}

func makeDomainSnapshot(item state.FilesItem) DomainSnapshot {
	var accessorIndexOpt *silkworm_go.MemoryMappedFile
	if item.AccessorIndex() != nil {
		accessorIndex := memoryMappedFile(item.AccessorIndex())
		accessorIndexOpt = &accessorIndex
	}
	return DomainSnapshot{
		Segment: memoryMappedFile(item.Segment()),
		ExistenceIndex: silkworm_go.MemoryMappedFile{
			FilePath:   NewFilePath(item.ExistenceFilter().FilePath),
			DataHandle: nil,
			Size:       0,
		},
		BTreeIndex:    memoryMappedFile(item.BtIndex()),
		AccessorIndex: accessorIndexOpt,
	}
}

func (r *SnapshotsRepository) updateState(stateTx *state.AggregatorRoTx) error {
	mergeRange := state.NewMergeRange("", true, 0, math.MaxUint64)
	domainRanges := func(name kv.Domain) state.DomainRanges {
		return state.NewDomainRanges(
			name,
			*mergeRange,
			state.NewHistoryRanges(*mergeRange, *mergeRange),
			0,
		)
	}
	var allDomainRanges [kv.DomainLen]state.DomainRanges
	for i := 0; i < len(allDomainRanges); i++ {
		allDomainRanges[i] = domainRanges(kv.Domain(i))
	}
	iiRanges := make([]*state.MergeRange, stateTx.InvertedIndicesLen())
	for i := 0; i < len(iiRanges); i++ {
		iiRanges[i] = mergeRange
	}
	ranges := state.NewRanges(allDomainRanges, iiRanges)

	allFiles, err := stateTx.FilesInRange(&ranges)
	if err != nil {
		return err
	}

	iiNames := make(map[kv.InvertedIdx]int, len(iiRanges))
	for i := 0; i < len(iiRanges); i++ {
		iiNames[stateTx.InvertedIndexName(i)] = i
	}

	iiFilesLogAddresses := allFiles.InvertedIndexFiles(iiNames[kv.LogAddrIdx])
	iiFilesLogTopics := allFiles.InvertedIndexFiles(iiNames[kv.LogTopicIdx])
	iiFilesTracesFrom := allFiles.InvertedIndexFiles(iiNames[kv.TracesFromIdx])
	iiFilesTracesTo := allFiles.InvertedIndexFiles(iiNames[kv.TracesToIdx])

	historyFilesAccounts := allFiles.DomainHistoryFiles(kv.AccountsDomain)
	historyFilesStorage := allFiles.DomainHistoryFiles(kv.StorageDomain)
	historyFilesCode := allFiles.DomainHistoryFiles(kv.CodeDomain)
	historyFilesReceipts := allFiles.DomainHistoryFiles(kv.ReceiptDomain)

	historyIIFilesAccounts := allFiles.DomainInvertedIndexFiles(kv.AccountsDomain)
	historyIIFilesStorage := allFiles.DomainInvertedIndexFiles(kv.StorageDomain)
	historyIIFilesCode := allFiles.DomainInvertedIndexFiles(kv.CodeDomain)
	historyIIFilesReceipts := allFiles.DomainInvertedIndexFiles(kv.ReceiptDomain)

	domainFilesAccounts := allFiles.DomainFiles(kv.AccountsDomain)
	domainFilesStorage := allFiles.DomainFiles(kv.StorageDomain)
	domainFilesCode := allFiles.DomainFiles(kv.CodeDomain)
	// TODO: enable after fixing .kvi configuration
	// domainFilesCommitment := allFiles.DomainFiles(kv.CommitmentDomain)
	domainFilesReceipts := allFiles.DomainFiles(kv.ReceiptDomain)

	countHistorical := len(iiFilesLogAddresses)
	if (len(iiFilesLogTopics) != countHistorical) || (len(iiFilesTracesFrom) != countHistorical) || (len(iiFilesTracesTo) != countHistorical) ||
		(len(historyFilesAccounts) != countHistorical) || (len(historyFilesStorage) != countHistorical) || (len(historyFilesCode) != countHistorical) || (len(historyFilesReceipts) != countHistorical) ||
		(len(historyIIFilesAccounts) != countHistorical) || (len(historyIIFilesStorage) != countHistorical) || (len(historyIIFilesCode) != countHistorical) || (len(historyIIFilesReceipts) != countHistorical) {
		return errors.New("silkworm.SnapshotsRepository.updateState: the number of historical files must be the same")
	}

	startTimeHistorical := time.Now()
	for i := 0; i < countHistorical; i++ {
		r.logger.Trace("[silkworm] snapshots updating historical", "i", i, "count", countHistorical)
		err := r.silkworm.AddStateSnapshotBundleHistorical(StateSnapshotBundleHistorical{
			Accounts: makeHistorySnapshot(historyFilesAccounts[i], historyIIFilesAccounts[i]),
			Storage:  makeHistorySnapshot(historyFilesStorage[i], historyIIFilesStorage[i]),
			Code:     makeHistorySnapshot(historyFilesCode[i], historyIIFilesCode[i]),
			Receipts: makeHistorySnapshot(historyFilesReceipts[i], historyIIFilesReceipts[i]),

			LogAddresses: makeInvertedIndexSnapshot(iiFilesLogAddresses[i]),
			LogTopics:    makeInvertedIndexSnapshot(iiFilesLogTopics[i]),
			TracesFrom:   makeInvertedIndexSnapshot(iiFilesTracesFrom[i]),
			TracesTo:     makeInvertedIndexSnapshot(iiFilesTracesTo[i]),
		})
		if err != nil {
			return err
		}
	}
	r.logger.Debug("[silkworm] snapshots updated historical", "count", countHistorical, "duration", time.Since(startTimeHistorical))

	countLatest := len(domainFilesAccounts)
	if (len(domainFilesStorage) != countLatest) || (len(domainFilesCode) != countLatest) ||
		/* TODO: enable after fixing .kvi configuration */
		/* (len(domainFilesCommitment) != countLatest) || */
		(len(domainFilesReceipts) != countLatest) {
		return errors.New("silkworm.SnapshotsRepository.updateState: the number of latest files must be the same")
	}

	startTimeLatest := time.Now()
	for i := 0; i < countLatest; i++ {
		r.logger.Trace("[silkworm] snapshots updating latest", "i", i, "count", countLatest)
		err := r.silkworm.AddStateSnapshotBundleLatest(StateSnapshotBundleLatest{
			Accounts: makeDomainSnapshot(domainFilesAccounts[i]),
			Storage:  makeDomainSnapshot(domainFilesStorage[i]),
			Code:     makeDomainSnapshot(domainFilesCode[i]),
			/* TODO: enable after fixing .kvi configuration */
			/* Commitment: makeDomainSnapshot(domainFilesCommitment[i]), */
			Commitment: makeDomainSnapshot(domainFilesReceipts[i]),
			Receipts:   makeDomainSnapshot(domainFilesReceipts[i]),
		})
		if err != nil {
			return err
		}
	}
	r.logger.Debug("[silkworm] snapshots updated latest", "count", countLatest, "duration", time.Since(startTimeLatest))

	return nil
}
