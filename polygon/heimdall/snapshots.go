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

package heimdall

import (
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/eth/ethconfig"
)

// Bor Events
// value: event_rlp
// bor_transaction_hash  -> bor_event_segment_offset

// Bor Spans
// value: span_json
// span_id -> offset

type RoSnapshots struct {
	snapshotsync.RoSnapshots
}

// NewBorRoSnapshots - opens all bor snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to] semantic
func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, logger log.Logger) *RoSnapshots {
	return &RoSnapshots{*snapshotsync.NewRoSnapshots(cfg, snapDir, SnapshotTypes(), false, logger)}
}

func (s *RoSnapshots) Ranges() []snapshotsync.Range {
	view := s.View()
	defer view.Close()
	return view.base.Ranges()
}

type View struct {
	base *snapshotsync.View
}

func (s *RoSnapshots) View() *View {
	v := &View{base: s.RoSnapshots.View().WithBaseSegType(Spans)}
	return v
}

func (v *View) Close() {
	v.base.Close()
}

func (v *View) Events() []*snapshotsync.VisibleSegment { return v.base.Segments(Events) }
func (v *View) Spans() []*snapshotsync.VisibleSegment  { return v.base.Segments(Spans) }
func (v *View) Checkpoints() []*snapshotsync.VisibleSegment {
	return v.base.Segments(Checkpoints)
}
func (v *View) Milestones() []*snapshotsync.VisibleSegment {
	return v.base.Segments(Milestones)
}

func (v *View) EventsSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(Events, blockNum)
}

func (v *View) SpansSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(Spans, blockNum)
}
