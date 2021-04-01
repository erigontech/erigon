package snapshotsync

import "testing"


func TestBuildHeadersSnapshot(t *testing.T) {
	sb:=&SnapshotMigrator{}
	sb.CreateHeadersSnapshot()
}
