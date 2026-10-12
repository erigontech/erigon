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

//go:build unix

package app

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/protocol/params"
)

func TestVerifyPBTScratchIOHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_VERIFY_PBT_IO_HELPER") != "1" {
		return
	}
	limit := &syscall.Rlimit{Cur: 2 << 20, Max: 2 << 20}
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, limit); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	err := verifyPBTFilesWithMaxCodeSize(t.Context(), os.Getenv("VERIFY_PBT_DATADIR"), os.Getenv("VERIFY_PBT_SNAPSHOT"), os.Getenv("VERIFY_PBT_PREIMAGES"), 0, params.MaxCodeSizeAmsterdam, os.Getenv("VERIFY_PBT_TMPDIR"))
	if err == nil || errors.Is(err, errVerifyPBTInvalid) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(0)
}

func TestVerifyPBTClassifiesMidstreamScratchIO(t *testing.T) {
	snapshot, preimages, root := buildMeasuredPBT(t, 10_000)
	anchor := newMeasuredPBTAnchor(t, root)
	scratch := filepath.Join(t.TempDir(), "scratch")
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestVerifyPBTScratchIOHelperProcess$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_VERIFY_PBT_IO_HELPER=1",
		"VERIFY_PBT_DATADIR="+anchor,
		"VERIFY_PBT_SNAPSHOT="+snapshot,
		"VERIFY_PBT_PREIMAGES="+preimages,
		"VERIFY_PBT_TMPDIR="+scratch,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), scratch)
}
