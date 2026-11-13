// Copyright 2026 The Erigon Authors
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

package state

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func BenchmarkDomainBuildResumeStep(b *testing.B) {
	configs := []statecfg.DomainCfg{statecfg.Schema.CommitmentDomain, statecfg.Schema.RCacheDomain}
	for i := range configs {
		cfg := &configs[i]
		cfg.Hist.IiCfg.Enabled = true
		b.Run(cfg.Name.String(), func(b *testing.B) {
			db, d := testDbAndDomainOfStep(b, *cfg, 16, log.New())
			tx, err := db.BeginRw(b.Context())
			require.NoError(b, err)
			defer tx.Rollback()
			view := d.beginForTests()
			defer view.Close()
			writer := view.NewWriter(db)
			defer writer.Close()
			for i := range uint64(100_000) {
				step := uint64(8)
				if i == 0 {
					step++
				}
				require.NoError(b, writer.PutWithPrev(binary.BigEndian.AppendUint64(nil, i), []byte{1}, step*d.stepSize+1, nil))
			}
			require.NoError(b, writer.Flush(b.Context(), tx))
			for _, tc := range []struct {
				name          string
				filesEnd      kv.Step
				minResumeStep kv.Step
				want          kv.Step
			}{
				{name: "files_end_0", want: 8},
				{name: "files_end_8", filesEnd: 8, want: 8},
				{name: "files_end_9", filesEnd: 9, want: 9},
				{name: "deferred", minResumeStep: 10, want: 8},
			} {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						first, found, err := d.firstStepToBuild(tx, tc.filesEnd, tc.minResumeStep)
						validStep := first == tc.want || (first > tc.want && first < tc.minResumeStep)
						if err != nil || !found || !validStep {
							b.Fatalf("step=%d found=%t err=%v", first, found, err)
						}
					}
				})
			}
		})
	}
}
