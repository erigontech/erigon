package state

// SetBlockEndChunks overrides the per-worker chunk sizes of NotifyAccumulator and Matches.
func SetBlockEndChunks(notify, matches int) {
	notifyWriteSetsPerWorker, matchesPerWorker = notify, matches
}
