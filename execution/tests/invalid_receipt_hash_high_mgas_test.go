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

package executiontests

import (
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
)

func TestInvalidReceiptHashHighMgas(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	if runtime.GOOS == "windows" {
		t.Skip("windows CI is extremely slow")
	}
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlCrit)
	testDir := path.Join(cornersDir, "invalid-receipt-hash-high-mgas")
	preAllocsDir := path.Join(testDir, "pre_alloc")
	payloadsDir := path.Join(testDir, "payloads")
	engineXRunner, err := NewEngineXTestRunner(t, logger, preAllocsDir)
	require.NoError(t, err)
	tm := testMatcher{}
	tm.walk(t, payloadsDir, func(t *testing.T, name string, test EngineXTestDefinition) {
		err := engineXRunner.Run(ctx, test)
		require.NoError(t, err)
	})
}
