package state

// SyncingInfo stores information regarding the syncing status of the node
type SyncingInfo struct {
	InitialSyncingBlock         uint64
	LastBlockNumberSeen         uint64
	LastBlockNumberConsolidated uint64
	CurrentBlockNumber          uint64

	InitialSyncingBatch         uint64
	LastBatchNumberSeen         uint64
	LastBatchNumberConsolidated uint64
	CurrentBatchNumber          uint64
}
