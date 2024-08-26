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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
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

		log.Info("[recompress_migration] 'recompressDomain' mode start")
		dirs2 := dirs
		dirs2.SnapDomain += "_v2"
		dir.MustExist(dirs2.SnapDomain)
		if err := rclone(logger, dirs.SnapDomain, dirs2.SnapDomain); err != nil {
			return err
		}
		files, err := storageFiles(dirs)
		if err != nil {
			return err
		}
		for _, from := range files {
			_, fromFileName := filepath.Split(from)
			fromStep, toStep, err := state.ParseStepsFromFileName(fromFileName)
			if err != nil {
				return err
			}
			if toStep-fromStep < state.DomainMinStepsToCompress {
				continue
			}

			to := filepath.Join(dirs2.SnapDomain, fromFileName)
			if err := recompressDomain(ctx, dirs2, from, to, logger); err != nil {
				return err
			}
			_ = os.Remove(strings.Replace(to, ".kv", ".bt", -1))
			_ = os.Remove(strings.Replace(to, ".kv", ".kvei", -1))
			_ = os.Remove(strings.Replace(to, ".kv", ".bt.torrent", -1))
			_ = os.Remove(strings.Replace(to, ".kv", ".kv.torrent", -1))
		}

		agg, err := state.NewAggregator(ctx, dirs, config3.HistoryV3AggregationStep, db, nil, logger)
		if err != nil {
			return err
		}
		defer agg.Close()
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
		if err = agg.OpenFolder(); err != nil {
			return err
		}
		if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		ac := agg.BeginFilesRo()
		defer ac.Close()

		aggV2, err := state.NewAggregator(ctx, dirs2, config3.HistoryV3AggregationStep, db, nil, logger)
		if err != nil {
			panic(err)
		}
		defer aggV2.Close()
		if err = aggV2.OpenFolder(); err != nil {
			panic(err)
		}
		aggV2.SetCompressWorkers(estimate.CompressSnapshot.Workers())
		if err := aggV2.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}

		acV2 := aggV2.BeginFilesRo()
		defer acV2.Close()

		if err = ac.SqueezeCommitmentFiles(acV2); err != nil {
			return err
		}
		acV2.Close()
		ac.Close()
		if err := agg.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		if err := aggV2.BuildMissedIndices(ctx, estimate.IndexSnapshot.Workers()); err != nil {
			return err
		}
		agg.Close()
		aggV2.Close()

		log.Info("[recompress] rename", "from", dirs.SnapDomain, "to", dirs.SnapDomain+"_old")
		_ = os.Remove(dirs.SnapDomain + "_old")
		if err := os.Rename(dirs.SnapDomain, dirs.SnapDomain+"_old"); err != nil {
			return err
		}
		log.Info("[recompress] rename", "from", dirs2.SnapDomain, "to", dirs.SnapDomain)
		if err := os.Rename(dirs2.SnapDomain, dirs.SnapDomain); err != nil {
			return err
		}

		return db.Update(ctx, func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}

func recompressDomain(ctx context.Context, dirs datadir.Dirs, from, to string, logger log.Logger) error {
	decompressor, err := seg.NewDecompressor(from)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.EnableReadAhead().DisableReadAhead()
	r := state.NewArchiveGetter(decompressor.MakeGetter(), state.DetectCompressType(decompressor.MakeGetter()))

	compressCfg := state.DomainCompressCfg
	compressCfg.Workers = estimate.CompressSnapshot.Workers()
	c, err := seg.NewCompressor(ctx, "recompressDomain", to, dirs.Tmp, compressCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer c.Close()
	w := state.NewArchiveWriter(c, state.CompressKeys)
	var k, v []byte
	var i int
	for r.HasNext() {
		i++
		k, _ = r.Next(k[:0])
		v, _ = r.Next(v[:0])
		if err = w.AddWord(k); err != nil {
			return err
		}
		if err = w.AddWord(v); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := c.Compress(); err != nil {
		return err
	}

	return nil
}

func storageFiles(dirs datadir.Dirs) ([]string, error) {
	files, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range files {
		if !strings.Contains(f, kv.StorageDomain.String()) {
			continue
		}
		res = append(res, f)
	}
	return res, nil
}

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
