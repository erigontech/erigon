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

package commands

import (
	"bytes"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestPBTFileStagesReportIntermediateProgress(t *testing.T) {
	configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
	source, _ := newPBTConversionSource(t)
	addPBTGoldenStorageBucket(t, source)
	previousHandler := log.Root().GetHandler()
	previousInterval := state.PBTProgressInterval
	var logs bytes.Buffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	state.PBTProgressInterval = time.Nanosecond
	t.Cleanup(func() {
		log.Root().SetHandler(previousHandler)
		state.PBTProgressInterval = previousInterval
	})
	require.NoError(t, convertPBT(t.Context(), source.DataDir, filepath.Join(t.TempDir(), "published"), true, "", log.New()))
	for _, suffix := range []string{".kv", ".idt", ".kvi"} {
		t.Run(suffix, func(t *testing.T) {
			phase := "range 0-1 " + suffix
			pattern := regexp.MustCompile(`phase="` + regexp.QuoteMeta(phase) + `" done=(\d+) .* total=(\d+)`)
			matches := pattern.FindAllStringSubmatch(logs.String(), -1)
			require.NotEmpty(t, matches, "missing phase %s", phase)
			var intermediate int
			var counts []string
			for _, match := range matches {
				done, err := strconv.ParseUint(match[1], 10, 64)
				require.NoError(t, err)
				total, err := strconv.ParseUint(match[2], 10, 64)
				require.NoError(t, err)
				counts = append(counts, match[1]+"/"+match[2])
				if done > 0 && done < total {
					intermediate++
				}
			}
			require.Positive(t, intermediate, "%s must report during its work loop; saw done/total=%v", phase, counts)
		})
	}
}
