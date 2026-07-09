// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package blocksnapshots

import (
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/node/ethconfig"
)

type RoSnapshots struct {
	snapshotsync.BaseRoSnapshots
}

// NewRoSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, logger log.Logger) *RoSnapshots {
	return &RoSnapshots{*snapshotsync.NewBaseRoSnapshots(cfg, snapDir, snaptype2.BlockSnapshotTypes, true, logger)}
}

type View struct {
	base *snapshotsync.View
}

func (s *RoSnapshots) View() *View {
	return &View{base: s.BaseRoSnapshots.View().WithBaseSegType(snaptype2.Transactions)}
}

func (v *View) Close() {
	v.base.Close()
}

func (v *View) Headers() []*snapshotsync.VisibleSegment { return v.base.Segments(snaptype2.Headers) }
func (v *View) Bodies() []*snapshotsync.VisibleSegment  { return v.base.Segments(snaptype2.Bodies) }
func (v *View) Txs() []*snapshotsync.VisibleSegment {
	return v.base.Segments(snaptype2.Transactions)
}

// Segment returns the segment of type t covering blockNum, if any.
func (v *View) Segment(t snaptype.Type, blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(t, blockNum)
}

func (v *View) HeadersSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(snaptype2.Headers, blockNum)
}

func (v *View) BodiesSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(snaptype2.Bodies, blockNum)
}
func (v *View) TxsSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(snaptype2.Transactions, blockNum)
}
