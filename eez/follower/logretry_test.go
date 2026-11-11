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

package follower

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
)

// TestLogRetryLevels: a missing blob retries at debug level for its first
// 30 seconds, at warning level up to a minute, then at error level; a lost
// blob at error level at once; any other failure at warning level. Only a
// debug-level retry is quiet.
func TestLogRetryLevels(t *testing.T) {
	missing := fmt.Errorf("batch 01…02: %w: slot 7 of L1 block 9", l1.ErrBlobUnavailable)
	lost := fmt.Errorf("batch 01…02: %w: 03 of L1 block 9", l1.ErrBlobLost)
	other := errors.New("L1 unreachable")
	for _, tc := range []struct {
		name string
		err  error
		age  time.Duration
		lvl  string
		loud bool
	}{
		{"blob missing", missing, 0, "dbug", false},
		{"blob missing for 29s", missing, 29 * time.Second, "dbug", false},
		{"blob missing for 31s", missing, 31 * time.Second, "warn", true},
		{"blob missing for 59s", missing, 59 * time.Second, "warn", true},
		{"blob missing for 61s", missing, 61 * time.Second, "eror", true},
		{"blob lost", lost, 0, "eror", true},
		{"other failure", other, 0, "warn", true},
		{"other failure for 2m", other, 2 * time.Minute, "warn", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			logger := log.New()
			logger.SetHandler(log.StreamHandler(&out, log.LogfmtFormat()))
			f := &Follower{logger: logger}
			loud := f.logRetry("[eez] follower step failed, retrying", tc.err, time.Now().Add(-tc.age), "in", time.Second)
			require.Equal(t, tc.loud, loud)
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			require.Len(t, lines, 1)
			require.Contains(t, lines[0], "lvl="+tc.lvl+" ")
			require.Contains(t, lines[0], tc.err.Error())
		})
	}
}
