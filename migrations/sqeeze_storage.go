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

package migrations

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
)

var EnableRecompressCommitmentFiles = false

var RecompressCommitmentFiles = Migration{
	Name: "recompress_commit_files",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		ctx := context.Background()

		if !EnableRecompressCommitmentFiles {
			log.Info("[recompress_migration] disabled")
			return db.Update(ctx, func(tx kv.RwTx) error {
				return BeforeCommit(tx, nil, true)
			})
		}

		logEvery := time.NewTicker(10 * time.Second)
		defer logEvery.Stop()
		t := time.Now()
		defer func() {
			log.Info("[recompress_migration] done", "took", time.Since(t))
		}()

		agg, err := state.NewAggregator(ctx, dirs, config3.HistoryV3AggregationStep, db, nil, logger)
		if err != nil {
			return err
		}
		defer agg.Close()
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())

		log.Info("[recompress_migration] start")
		dirsOld := dirs
		dirsOld.SnapDomain += "_old"
		dir.MustExist(dirsOld.SnapDomain, dirs.SnapDomain+"_backup")
		//TODO: `rclone` func doesn't work for big files. need to debug
		//if err := rclone(logger, dirs.SnapDomain, dirsOld.SnapDomain); err != nil {
		//	return err
		//}
		//if err := rclone(logger, dirs.SnapDomain, dirs.SnapDomain+"_backup"); err != nil {
		//	return err
		//}
		if err := agg.Sqeeze(ctx, kv.StorageDomain); err != nil {
			return err
		}

		if err = agg.OpenFolder(); err != nil {
			return err
		}
		if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		ac := agg.BeginFilesRo()
		defer ac.Close()

		aggOld, err := state.NewAggregator(ctx, dirsOld, config3.HistoryV3AggregationStep, db, nil, logger)
		if err != nil {
			panic(err)
		}
		defer aggOld.Close()
		if err = aggOld.OpenFolder(); err != nil {
			panic(err)
		}
		aggOld.SetCompressWorkers(estimate.CompressSnapshot.Workers())
		if err := aggOld.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}

		acOld := aggOld.BeginFilesRo()
		defer acOld.Close()

		if err = acOld.SqueezeCommitmentFiles(ac); err != nil {
			return err
		}
		acOld.Close()
		ac.Close()
		if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		if err := aggOld.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		agg.Close()
		aggOld.Close()

		log.Info("[recompress] removing", "dir", dirsOld.SnapDomain)
		_ = os.RemoveAll(dirsOld.SnapDomain)
		log.Info("[recompress] success", "please_remove", dirs.SnapDomain+"_backup")
		return db.Update(ctx, func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}

func domainFiles(dirs datadir.Dirs, domain kv.Domain) []string {
	files, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	if err != nil {
		panic(err)
	}
	res := make([]string, 0, len(files))
	for _, f := range files {
		if !strings.Contains(f, domain.String()) {
			continue
		}
		res = append(res, f)
	}
	return res
}

// nolint
func rclone(logger log.Logger, from, to string) error {
	cmd := exec.Command("rclone", "sync", "--progress", "--stats-one-line", from, to)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return err
	}

	// WaitGroup to wait for both goroutines to finish
	var wg sync.WaitGroup
	wg.Add(2)

	// Stream stdout
	go func() {
		defer wg.Done()
		streamToLogger(stdoutPipe, logger, "STDOUT")
	}()

	// Stream stderr
	go func() {
		defer wg.Done()
		streamToLogger(stderrPipe, logger, "STDERR")
	}()

	// Wait for all streams to finish
	wg.Wait()
	return nil
}

// streamToLogger reads from the provided reader and logs each line
func streamToLogger(reader io.Reader, logger log.Logger, prefix string) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		logger.Info("[recompress] rclone", "out", prefix, "text", scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		logger.Info("[recompress] rclone", "out", prefix, "err", err)
	}
}
