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

//go:build !windows

package commands

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

type recordingHandler struct {
	mu           sync.Mutex
	records      []log.Record
	once         sync.Once
	onConvertLog func()
}

func (h *recordingHandler) Log(r *log.Record) error {
	if h.onConvertLog != nil && strings.HasPrefix(r.Msg, "[commitment_convert]") {
		h.once.Do(h.onConvertLog)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, *r)
	return nil
}

func (h *recordingHandler) Enabled(context.Context, log.Lvl) bool { return true }

func (h *recordingHandler) withMsg(msg string) []log.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var found []log.Record
	for _, r := range h.records {
		if r.Msg == msg {
			found = append(found, r)
		}
	}
	return found
}

func recordConsoleLogs(t *testing.T) *recordingHandler {
	t.Helper()
	recorder := &recordingHandler{}
	previousConsole, previousRoot := log.StderrHandler, log.Root().GetHandler()
	log.StderrHandler = recorder
	t.Cleanup(func() {
		log.StderrHandler = previousConsole
		log.Root().SetHandler(previousRoot)
	})
	return recorder
}

func TestCommitmentConvertWithoutChaindataWarnsWhenTheThrowawayDBCannotBeRemoved(t *testing.T) {
	require.NotZero(t, os.Geteuid(), "root ignores directory permissions, so the removal cannot fail")
	dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
	throwawayDirs := filepath.Join(dirs.Tmp, "commitment-convert-*")
	t.Cleanup(func() {
		left, _ := filepath.Glob(throwawayDirs)
		for _, d := range left {
			_ = os.Chmod(d, 0o700)
		}
	})
	blocked := make(chan string, 1)
	logs := recordConsoleLogs(t)
	logs.onConvertLog = func() {
		left, err := filepath.Glob(throwawayDirs)
		if err != nil || len(left) != 1 || os.Chmod(left[0], 0o500) != nil {
			blocked <- ""
			return
		}
		blocked <- left[0]
	}

	require.NoError(t, runCommitmentConvert(t, dirs, "--v3"))

	var throwawayDir string
	select {
	case throwawayDir = <-blocked:
	default:
	}
	require.NotEmpty(t, throwawayDir, "the throwaway DB was never made read-only")
	requireCommitmentKVPrefix(t, dirs, "v3.")
	require.DirExists(t, throwawayDir)
	warned := logs.withMsg("[commitment_convert] removing the throwaway DB failed")
	require.Len(t, warned, 1)
	require.Equal(t, log.LvlWarn, warned[0].Lvl)
	require.Len(t, warned[0].Ctx, 4)
	require.Equal(t, []any{"dir", throwawayDir, "err"}, warned[0].Ctx[:3])
	removeErr, ok := warned[0].Ctx[3].(error)
	require.True(t, ok)
	require.ErrorIs(t, removeErr, fs.ErrPermission)
}
