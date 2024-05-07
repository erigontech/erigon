package downloader_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/log/v3"
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
