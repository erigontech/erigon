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

package cmp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/erigon-lib/log/v3"

	"github.com/erigontech/erigon/erigon-lib/chain"
	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/downloader"
	"github.com/erigontech/erigon/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon/cmd/snapshots/flags"
	"github.com/erigontech/erigon/cmd/snapshots/sync"
	"github.com/erigontech/erigon/cmd/utils"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/logging"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var Command = cli.Command{
	Action:    cmp,
	Name:      "cmp",
	Usage:     "Compare snapshot segments",
	ArgsUsage: "<start block> <end block>",
	Flags: []cli.Flag{
		&flags.SegTypes,
		&utils.DataDirFlag,
		&logging.LogVerbosityFlag,
		&logging.LogConsoleVerbosityFlag,
		&logging.LogDirVerbosityFlag,
		&utils.WebSeedsFlag,
		&utils.NATFlag,
		&utils.DisableIPV6,
		&utils.DisableIPV4,
		&utils.TorrentDownloadRateFlag,
		&utils.TorrentUploadRateFlag,
		&utils.TorrentVerbosityFlag,
		&utils.TorrentPortFlag,
		&utils.TorrentMaxPeersFlag,
		&utils.TorrentConnsPerFileFlag,
	},
	Description: ``,
}

func cmp(cliCtx *cli.Context) error {

	logger := sync.Logger(cliCtx.Context)

	var loc1, loc2 *sync.Locator

	var rcCli *downloader.RCloneClient
	var torrentCli *sync.TorrentClient

	dataDir := cliCtx.String(utils.DataDirFlag.Name)
	var tempDir string

	if len(dataDir) == 0 {
		dataDir, err := os.MkdirTemp("", "snapshot-cpy-")
		if err != nil {
			return err
		}
		tempDir = dataDir
		defer os.RemoveAll(dataDir)
	} else {
		tempDir = filepath.Join(dataDir, "temp")

		if err := os.MkdirAll(tempDir, 0755); err != nil {
			return err
		}
	}

	cliCtx.Context = sync.WithTempDir(cliCtx.Context, tempDir)

	var err error

	checkRemote := func(src string) error {
		if rcCli == nil {
			rcCli, err = downloader.NewRCloneClient(logger)

			if err != nil {
				return err
			}
		}

		return sync.CheckRemote(rcCli, src)
	}

	var chain string

	pos := 0

	if cliCtx.Args().Len() > pos {
		val := cliCtx.Args().Get(pos)

		if loc1, err = sync.ParseLocator(val); err != nil {
			return err
		}

		switch loc1.LType {
		case sync.RemoteFs:
			if err = checkRemote(loc1.Src); err != nil {
				return err
			}

			chain = loc1.Chain
		}
	}

	pos++

	if cliCtx.Args().Len() > pos {
		val := cliCtx.Args().Get(pos)

		if loc2, err = sync.ParseLocator(val); err != nil {
			return err
		}

		switch loc2.LType {
		case sync.RemoteFs:
			if err = checkRemote(loc2.Src); err != nil {
				return err
			}

			chain = loc2.Chain
		}

		pos++
	}

	if loc1.LType == sync.TorrentFs || loc2.LType == sync.TorrentFs {
		config := sync.NewTorrentClientConfigFromCobra(cliCtx, chain)
		torrentCli, err = sync.NewTorrentClient(cliCtx.Context, config)
		if err != nil {
			return fmt.Errorf("can't create torrent: %w", err)
		}
	}

	typeValues := cliCtx.StringSlice(flags.SegTypes.Name)
	snapTypes := make([]snaptype.Type, 0, len(typeValues))

	for _, val := range typeValues {
		segType, ok := snaptype.ParseFileType(val)

		if !ok {
			return fmt.Errorf("unknown file type: %s", val)
		}

		snapTypes = append(snapTypes, segType)
	}

	var firstBlock, lastBlock uint64

	if cliCtx.Args().Len() > pos {
		firstBlock, err = strconv.ParseUint(cliCtx.Args().Get(0), 10, 64)
	}

	if cliCtx.Args().Len() > 1 {
		lastBlock, err = strconv.ParseUint(cliCtx.Args().Get(1), 10, 64)
	}

	var session1 sync.DownloadSession
	var session2 sync.DownloadSession

	if rcCli != nil {
		if loc1.LType == sync.RemoteFs {
			session1, err = rcCli.NewSession(cliCtx.Context, filepath.Join(tempDir, "l1"), loc1.Src+":"+loc1.Root, nil)

			if err != nil {
				return err
			}
		}

		if loc2.LType == sync.RemoteFs {
			session2, err = rcCli.NewSession(cliCtx.Context, filepath.Join(tempDir, "l2"), loc2.Src+":"+loc2.Root, nil)

			if err != nil {
				return err
			}
		}
	}

	if torrentCli != nil {
		if loc1.LType == sync.TorrentFs {
			session1 = sync.NewTorrentSession(torrentCli, chain)
		}

		if loc2.LType == sync.TorrentFs {
			session2 = sync.NewTorrentSession(torrentCli, chain)
		}
	}

	if session1 == nil {
		return errors.New("no first session established")
	}

	if session1 == nil {
		return errors.New("no second session established")
	}

	logger.Info(fmt.Sprintf("Starting compare: %s==%s", loc1.String(), loc2.String()), "first", firstBlock, "last", lastBlock, "types", snapTypes, "dir", tempDir)

	logger.Info("Reading s1 dir", "remoteFs", session1.RemoteFsRoot(), "label", session1.Label())
	files, err := sync.DownloadManifest(cliCtx.Context, session1)

	if err != nil {
		files, err = session1.ReadRemoteDir(cliCtx.Context, true)
	}

	if err != nil {
		return err
	}

	h1ents, b1ents := splitEntries(files, loc1.Version, firstBlock, lastBlock)

	logger.Info("Reading s2 dir", "remoteFs", session2.RemoteFsRoot(), "label", session2.Label())
	files, err = sync.DownloadManifest(cliCtx.Context, session2)

	if err != nil {
		files, err = session2.ReadRemoteDir(cliCtx.Context, true)
	}

	if err != nil {
		return err
	}

	h2ents, b2ents := splitEntries(files, loc2.Version, firstBlock, lastBlock)

	c := comparitor{
		chain:    chain,
		loc1:     loc1,
		loc2:     loc2,
		session1: session1,
		session2: session2,
	}

	var funcs []func(ctx context.Context) (time.Duration, time.Duration, time.Duration, error)

	bodyWorkers := 4
	headerWorkers := 4

	if len(snapTypes) == 0 {
		funcs = append(funcs, func(ctx context.Context) (time.Duration, time.Duration, time.Duration, error) {
			return c.compareHeaders(ctx, h1ents, h2ents, headerWorkers, logger)
		}, func(ctx context.Context) (time.Duration, time.Duration, time.Duration, error) {
			return c.compareBodies(ctx, b1ents, b2ents, bodyWorkers, logger)
		})
	} else {
		for _, snapType := range snapTypes {
			if snapType.Enum() == coresnaptype.Enums.Headers {
				funcs = append(funcs, func(ctx context.Context) (time.Duration, time.Duration, time.Duration, error) {
					return c.compareHeaders(ctx, h1ents, h2ents, headerWorkers, logger)
				})
			}

			if snapType.Enum() == coresnaptype.Enums.Bodies {
				funcs = append(funcs, func(ctx context.Context) (time.Duration, time.Duration, time.Duration, error) {
					return c.compareBodies(ctx, b1ents, b2ents, bodyWorkers, logger)
				})
			}
		}
	}

	if len(funcs) > 0 {
		startTime := time.Now()

		var downloadTime uint64
		var indexTime uint64
		var compareTime uint64

		g, ctx := errgroup.WithContext(cliCtx.Context)
		g.SetLimit(len(funcs))

		for _, f := range funcs {
			func(ctx context.Context, f func(ctx context.Context) (time.Duration, time.Duration, time.Duration, error)) {
				g.Go(func() error {
					dt, it, ct, err := f(ctx)

					atomic.AddUint64(&downloadTime, uint64(dt))
					atomic.AddUint64(&indexTime, uint64(it))
					atomic.AddUint64(&compareTime, uint64(ct))

					return err
				})
			}(ctx, f)
		}

		err = g.Wait()

		if err == nil {
			logger.Info(fmt.Sprintf("Finished compare: %s==%s", loc1.String(), loc2.String()), "elapsed", time.Since(startTime),
				"downloading", time.Duration(downloadTime), "indexing", time.Duration(indexTime), "comparing", time.Duration(compareTime))
		} else {
			logger.Info(fmt.Sprintf("Failed compare: %s==%s", loc1.String(), loc2.String()), "err", err, "elapsed", time.Since(startTime),
				"downloading", time.Duration(downloadTime), "indexing", time.Duration(indexTime), "comparing", time.Duration(compareTime))
		}

	}
	return nil
}

type BodyEntry struct {
	From, To           uint64
	Body, Transactions fs.DirEntry
}

func splitEntries(files []fs.DirEntry, version snaptype.Version, firstBlock, lastBlock uint64) (hents []fs.DirEntry, bents []*BodyEntry) {
	for _, ent := range files {
		if info, err := ent.Info(); err == nil {
			if snapInfo, ok := info.Sys().(downloader.SnapInfo); ok && snapInfo.Version() > 0 {
				if version == snapInfo.Version() &&
					(firstBlock == 0 || snapInfo.From() >= firstBlock) &&
					(lastBlock == 0 || snapInfo.From() < lastBlock) {

					if snapInfo.Type().Enum() == coresnaptype.Enums.Headers {
						hents = append(hents, ent)
					}

					if snapInfo.Type().Enum() == coresnaptype.Enums.Bodies {
						found := false

						for _, bent := range bents {
							if snapInfo.From() == bent.From &&
								snapInfo.To() == bent.To {
								bent.Body = ent
								found = true
							}
						}

						if !found {
							bents = append(bents, &BodyEntry{snapInfo.From(), snapInfo.To(), ent, nil})
						}
					}

					if snapInfo.Type().Enum() == coresnaptype.Enums.Transactions {
						found := false

						for _, bent := range bents {
							if snapInfo.From() == bent.From &&
								snapInfo.To() == bent.To {
								bent.Transactions = ent
								found = true

							}
						}

						if !found {
							bents = append(bents, &BodyEntry{snapInfo.From(), snapInfo.To(), nil, ent})
						}
					}
				}
			}
		}
	}

	return hents, bents
}

type comparitor struct {
	chain      string
	loc1, loc2 *sync.Locator
	session1   sync.DownloadSession
	session2   sync.DownloadSession
}

func (c comparitor) chainConfig() *chain.Config {
	return params.ChainConfigByChainName(c.chain)
}

func (c comparitor) compareHeaders(ctx context.Context, f1ents []fs.DirEntry, f2ents []fs.DirEntry, workers int, logger log.Logger) (time.Duration, time.Duration, time.Duration, error) {
	var downloadTime uint64
	var compareTime uint64

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for i1, ent1 := range f1ents {
		var snapInfo1 downloader.SnapInfo

		if info, err := ent1.Info(); err == nil {
			snapInfo1, _ = info.Sys().(downloader.SnapInfo)
		}

		if snapInfo1 == nil {
			continue
		}

		for i2, ent2 := range f2ents {

			var snapInfo2 downloader.SnapInfo

			ent2Info, err := ent2.Info()

			if err == nil {
				snapInfo2, _ = ent2Info.Sys().(downloader.SnapInfo)
			}

			if snapInfo2 == nil ||
				snapInfo1.Type() != snapInfo2.Type() ||
				snapInfo1.From() != snapInfo2.From() ||
				snapInfo1.To() != snapInfo2.To() {
				continue
			}

			i1, i2, ent1, ent2 := i1, i2, ent1, ent2

			g.Go(func() error {
				g, gctx := errgroup.WithContext(ctx)
				g.SetLimit(2)

				g.Go(func() error {
					logger.Info("Downloading ", ent1.Name(), "entry", fmt.Sprint(i1+1, "/", len(f1ents)))
					startTime := time.Now()
					defer func() {
						atomic.AddUint64(&downloadTime, uint64(time.Since(startTime)))
					}()

					err := c.session1.Download(gctx, ent1.Name())

					if err != nil {
						return err
					}

					return nil
				})

				g.Go(func() error {
					startTime := time.Now()
					defer func() {
						atomic.AddUint64(&downloadTime, uint64(time.Since(startTime)))
					}()

					logger.Info("Downloading "+ent2.Name(), "entry", fmt.Sprint(i2+1, "/", len(f2ents)), "size", datasize.ByteSize(ent2Info.Size()))
					err := c.session2.Download(gctx, ent2.Name())

					if err != nil {
						return err
					}

					return nil
				})

				if err := g.Wait(); err != nil {
					return err
				}

				info1, _, _ := snaptype.ParseFileName(c.session1.LocalFsRoot(), ent1.Name())

				f1snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
					ProduceE2:    false,
					NoDownloader: true,
				}, info1.Dir(), info1.From, logger)

				f1snaps.OpenList([]string{ent1.Name()}, false)

				info2, _, _ := snaptype.ParseFileName(c.session2.LocalFsRoot(), ent1.Name())

				f2snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
					ProduceE2:    false,
					NoDownloader: true,
				}, info2.Dir(), info2.From, logger)

				f2snaps.OpenList([]string{ent2.Name()}, false)

				err = func() error {
					logger.Info(fmt.Sprintf("Comparing %s %s", ent1.Name(), ent2.Name()))
					startTime := time.Now()

					defer func() {
						atomic.AddUint64(&compareTime, uint64(time.Since(startTime)))
					}()

					blockReader1 := freezeblocks.NewBlockReader(f1snaps, nil, nil, nil)
					blockReader2 := freezeblocks.NewBlockReader(f2snaps, nil, nil, nil)

					g, gctx = errgroup.WithContext(ctx)
					g.SetLimit(2)

					h2chan := make(chan *types.Header)

					g.Go(func() error {
						blockReader2.HeadersRange(gctx, func(h2 *types.Header) error {
							select {
							case h2chan <- h2:
								return nil
							case <-gctx.Done():
								return gctx.Err()
							}
						})

						close(h2chan)
						return nil
					})

					g.Go(func() error {
						err := blockReader1.HeadersRange(gctx, func(h1 *types.Header) error {
							select {
							case h2 := <-h2chan:
								if h2 == nil {
									return fmt.Errorf("header %d unknown", h1.Number.Uint64())
								}

								if h1.Number.Uint64() != h2.Number.Uint64() {
									return fmt.Errorf("mismatched headers: expected %d, Got: %d", h1.Number.Uint64(), h2.Number.Uint64())
								}

								var h1buf, h2buf bytes.Buffer

								h1.EncodeRLP(&h1buf)
								h2.EncodeRLP(&h2buf)

								if !bytes.Equal(h1buf.Bytes(), h2buf.Bytes()) {
									return fmt.Errorf("%d: headers do not match", h1.Number.Uint64())
								}

								return nil
							case <-gctx.Done():
								return gctx.Err()
							}
						})

						return err
					})

					return g.Wait()
				}()

				files := f1snaps.OpenFiles()
				f1snaps.Close()

				files = append(files, f2snaps.OpenFiles()...)
				f2snaps.Close()

				for _, file := range files {
					os.Remove(file)
				}

				return err
			})
		}
	}

	err := g.Wait()

	return time.Duration(downloadTime), 0, time.Duration(compareTime), err
}

func (c comparitor) compareBodies(ctx context.Context, f1ents []*BodyEntry, f2ents []*BodyEntry, workers int, logger log.Logger) (time.Duration, time.Duration, time.Duration, error) {
	var downloadTime uint64
	var indexTime uint64
	var compareTime uint64

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for i1, ent1 := range f1ents {
		for i2, ent2 := range f2ents {
			if ent1.From != ent2.From ||
				ent1.To != ent2.To {
				continue
			}

			i1, i2, ent1, ent2 := i1, i2, ent1, ent2

			g.Go(func() error {
				g, ctx := errgroup.WithContext(ctx)
				g.SetLimit(4)

				b1err := make(chan error, 1)

				g.Go(func() error {

					info, _, ok := snaptype.ParseFileName(c.session1.LocalFsRoot(), ent1.Body.Name())

					err := func() error {
						startTime := time.Now()

						if !ok {
							return fmt.Errorf("can't parse file name %s", ent1.Body.Name())
						}

						defer func() {
							atomic.AddUint64(&downloadTime, uint64(time.Since(startTime)))
						}()

						logger.Info("Downloading "+ent1.Body.Name(), "entry", fmt.Sprint(i1+1, "/", len(f1ents)))
						return c.session1.Download(ctx, ent1.Body.Name())
					}()

					b1err <- err

					if err != nil {
						return fmt.Errorf("can't download %s: %w", ent1.Body.Name(), err)
					}

					startTime := time.Now()

					defer func() {
						atomic.AddUint64(&indexTime, uint64(time.Since(startTime)))
					}()

					logger.Info("Indexing " + ent1.Body.Name())

					return coresnaptype.Bodies.BuildIndexes(ctx, info, nil, c.chainConfig(), c.session1.LocalFsRoot(), nil, log.LvlDebug, logger)
				})

				g.Go(func() error {
					info, _, ok := snaptype.ParseFileName(c.session1.LocalFsRoot(), ent1.Transactions.Name())
					if !ok {
						return fmt.Errorf("can't parse file name %s", ent1.Transactions.Name())
					}

					err := func() error {
						startTime := time.Now()
						defer func() {
							atomic.AddUint64(&downloadTime, uint64(time.Since(startTime)))
						}()
						logger.Info("Downloading "+ent1.Transactions.Name(), "entry", fmt.Sprint(i1+1, "/", len(f1ents)))
						return c.session1.Download(ctx, ent1.Transactions.Name())
					}()

					if err != nil {
						return fmt.Errorf("can't download %s: %w", ent1.Transactions.Name(), err)
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case err = <-b1err:
						if err != nil {
							return fmt.Errorf("can't create transaction index: no bodies: %w", err)
						}
					}

					startTime := time.Now()

					defer func() {
						atomic.AddUint64(&indexTime, uint64(time.Since(startTime)))
					}()

					logger.Info("Indexing " + ent1.Transactions.Name())
					return coresnaptype.Transactions.BuildIndexes(ctx, info, nil, c.chainConfig(), c.session1.LocalFsRoot(), nil, log.LvlDebug, logger)
				})

				b2err := make(chan error, 1)

				g.Go(func() error {
					info, _, ok := snaptype.ParseFileName(c.session1.LocalFsRoot(), ent1.Body.Name())

					err := func() error {
						startTime := time.Now()

						if !ok {
							return fmt.Errorf("can't parse file name %s", ent1.Body.Name())
						}

						defer func() {
							atomic.AddUint64(&downloadTime, uint64(time.Since(startTime)))
						}()

						logger.Info("Downloading "+ent2.Body.Name(), "entry", fmt.Sprint(i2+1, "/", len(f2ents)))
						return c.session2.Download(ctx, ent2.Body.Name())
					}()

					b2err <- err

					if err != nil {
						return fmt.Errorf("can't download %s: %w", ent2.Body.Name(), err)
					}

					startTime := time.Now()

					defer func() {
						atomic.AddUint64(&indexTime, uint64(time.Since(startTime)))
					}()

					logger.Info("Indexing " + ent2.Body.Name())
					return coresnaptype.Bodies.BuildIndexes(ctx, info, nil, c.chainConfig(), c.session1.LocalFsRoot(), nil, log.LvlDebug, logger)
				})

				g.Go(func() error {
					info, _, ok := snaptype.ParseFileName(c.session1.LocalFsRoot(), ent1.Transactions.Name())

					err := func() error {
						startTime := time.Now()

						if !ok {
							return fmt.Errorf("can't parse file name %s", ent1.Transactions.Name())
						}

						defer func() {
							atomic.AddUint64(&downloadTime, uint64(time.Since(startTime)))
						}()

						logger.Info("Downloading "+ent2.Transactions.Name(), "entry", fmt.Sprint(i2+1, "/", len(f2ents)))
						return c.session2.Download(ctx, ent2.Transactions.Name())
					}()

					if err != nil {
						return fmt.Errorf("can't download %s: %w", ent2.Transactions.Name(), err)
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case err = <-b2err:
						if err != nil {
							return fmt.Errorf("can't create transaction index: no bodies: %w", err)
						}
					}

					startTime := time.Now()

					defer func() {
						atomic.AddUint64(&indexTime, uint64(time.Since(startTime)))
					}()

					logger.Info("Indexing " + ent2.Transactions.Name())
					return coresnaptype.Transactions.BuildIndexes(ctx, info, nil, c.chainConfig(), c.session2.LocalFsRoot(), nil, log.LvlDebug, logger)
				})

				if err := g.Wait(); err != nil {
					return err
				}

				info1, _, _ := snaptype.ParseFileName(c.session1.LocalFsRoot(), ent1.Body.Name())

				f1snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
					ProduceE2:    false,
					NoDownloader: true,
				}, info1.Dir(), info1.From, logger)

				f1snaps.OpenList([]string{ent1.Body.Name(), ent1.Transactions.Name()}, false)

				info2, _, _ := snaptype.ParseFileName(c.session2.LocalFsRoot(), ent2.Body.Name())

				f2snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
					ProduceE2:    false,
					NoDownloader: true,
				}, info2.Dir(), info2.From, logger)

				f2snaps.OpenList([]string{ent2.Body.Name(), ent2.Transactions.Name()}, false)

				err := func() error {
					logger.Info(fmt.Sprintf("Comparing %s %s", ent1.Body.Name(), ent2.Body.Name()))

					startTime := time.Now()

					defer func() {
						atomic.AddUint64(&compareTime, uint64(time.Since(startTime)))
					}()

					blockReader1 := freezeblocks.NewBlockReader(f1snaps, nil, nil, nil)
					blockReader2 := freezeblocks.NewBlockReader(f2snaps, nil, nil, nil)

					return func() error {
						for i := ent1.From; i < ent1.To; i++ {
							body1, err := blockReader1.BodyWithTransactions(ctx, nil, common.Hash{}, i)

							if err != nil {
								return fmt.Errorf("%d: can't get body 1: %w", i, err)
							}

							body2, err := blockReader2.BodyWithTransactions(ctx, nil, common.Hash{}, i)

							if err != nil {
								return fmt.Errorf("%d: can't get body 2: %w", i, err)
							}

							var b1buf, b2buf bytes.Buffer

							body1.EncodeRLP(&b1buf)
							body2.EncodeRLP(&b2buf)

							if !bytes.Equal(b1buf.Bytes(), b2buf.Bytes()) {
								return fmt.Errorf("%d: bodies do not match", i)
							}
						}

						return nil
					}()
				}()

				files := f1snaps.OpenFiles()
				f1snaps.Close()

				files = append(files, f2snaps.OpenFiles()...)
				f2snaps.Close()

				for _, file := range files {
					os.Remove(file)
				}

				return err
			})
		}
	}

	err := g.Wait()

	return time.Duration(downloadTime), time.Duration(indexTime), time.Duration(compareTime), err
}
