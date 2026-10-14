package mvcc

// RetireReason identifies why a file was removed from dirtyFiles.
type RetireReason int

const (
	RetireReasonMerged RetireReason = iota + 1
	RetireReasonAged
	RetireReasonWasDeletedFromDisk // imagine you are external RPCDaemon. It subscribing to `OnSnapshotsChanged` event. Erigon can delete from disk files which you still open - it's valid case.
	RetireReasonDemoted            // the same path now resolves to a cold-tier copy: close only, never delete
	// TODO: add one more reason for "we downloaded too much files - let's remove several recent files" case. We have it in stage_snapshots
)

func (r RetireReason) String() string {
	switch r {
	case RetireReasonMerged:
		return "merged"
	case RetireReasonAged:
		return "aged"
	case RetireReasonWasDeletedFromDisk:
		return "was-deleted-from-disk"
	case RetireReasonDemoted:
		return "demoted"
	default:
		return "unknown"
	}
}
