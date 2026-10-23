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
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestPBTProgressReportsRuntimeState(t *testing.T) {
	previousHandler := log.Root().GetHandler()
	var logs bytes.Buffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })
	progress := NewPBTProgress(PBTProgressConfig{Phase: "test", Total: 2, TempDir: t.TempDir(), Interval: time.Nanosecond})
	progress.Add(1)
	progress.Done()
	line := logs.String()
	require.Contains(t, line, "phase=test")
	require.Contains(t, line, "done=1")
	require.Contains(t, line, "total=2")
	require.Contains(t, line, "eta=")
	require.Contains(t, line, "rate=")
	require.Contains(t, line, "rss_anon=")
	require.Contains(t, line, "go_heap=")
	require.Contains(t, line, "temp_bytes=")
}
