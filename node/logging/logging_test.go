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

package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestSetupBannerGoesToConsoleOnly(t *testing.T) {
	var console bytes.Buffer
	stderrHandler := log.StderrHandler
	log.StderrHandler = log.StreamHandler(&console, log.TerminalFormatNoColor())
	t.Cleanup(func() { log.StderrHandler = stderrHandler })

	dirPath := t.TempDir()
	logger := log.New()
	initSeparatedLogging(logger, "test", dirPath, log.LvlInfo, log.LvlInfo, false, false)
	logger.Info("first line of the app")

	content, err := os.ReadFile(filepath.Join(dirPath, "test.log"))
	require.NoError(t, err)
	require.Contains(t, string(content), "first line of the app")
	require.NotContains(t, string(content), "logging to file system")
	require.Contains(t, console.String(), "logging to file system")
}
