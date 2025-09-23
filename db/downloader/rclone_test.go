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

package downloader_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/downloader"
)

func hasRClone() bool {
	rclone, _ := exec.LookPath("rclone")
	return len(rclone) != 0
}

func TestDownload(t *testing.T) {
	t.Skip()
	if !hasRClone() {
		t.Skip("rclone not available")
	}

	ctx := context.Background()

	tmpDir := t.TempDir()
	remoteDir := "r2:erigon-v2-snapshots-bor-mainnet"

	cli, err := downloader.NewRCloneClient(log.Root())

	if err != nil {
		t.Fatal(err)
	}

	rcc, err := cli.NewSession(ctx, tmpDir, remoteDir, nil)

	if err != nil {
		t.Fatal(err)
	}

	dir, err := rcc.ReadRemoteDir(ctx, true)

	if err != nil {
		if errors.Is(err, downloader.ErrAccessDenied) {
			t.Skip("rclone dir not accessible")
		}

		t.Fatal(err)
	}

	for _, entry := range dir {
		if len(entry.Name()) == 0 {
			t.Fatal("unexpected nil file name")
		}
		//fmt.Println(entry.Name())
	}

	err = rcc.Download(ctx, "manifest.txt")

	if err != nil {
		t.Fatal(err)
	}

	h0, err := os.ReadFile("manifest.txt")

	if err != nil {
		t.Fatal(err)
	}

	if len(h0) == 0 {
		t.Fatal("unexpected nil file")
	}
	//fmt.Print(string(h0))

	reader, err := rcc.Cat(ctx, "manifest.txt")

	if err != nil {
		t.Fatal(err)
	}

	h1, err := io.ReadAll(reader)

	if err != nil {
		t.Fatal(err)
	}

	if string(h0) != string(h1) {
		t.Fatal("Download and Cat contents mismatched")
	}
	//fmt.Print(string(h1))

	rcc.Stop()
}
