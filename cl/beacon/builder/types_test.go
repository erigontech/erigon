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

package builder

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestBlockValueRejectsOversizedInputBeforeParsing(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Root().GetHandler()
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previous) })

	value := strings.Repeat("9", 2<<20)
	header := ExecutionHeader{Data: ExecutionHeaderData{Message: ExecutionHeaderMessage{Value: value}}}
	start := time.Now()

	require.Nil(t, header.BlockValue())
	require.Less(t, time.Since(start), time.Second)
	require.Less(t, logs.Len(), 1024)
}
