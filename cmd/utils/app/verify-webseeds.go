package app

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"sync/atomic"
	"unsafe"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/generics/result"
	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

func verifyWebseeds(cliCtx *cli.Context) (err error) {
	var dirs datadir.Dirs
	if cliCtx.IsSet(utils.DataDirFlag.Name) {
		dirs = datadir.Open(cliCtx.String(utils.DataDirFlag.Name))
	}
	err = handlePreverifiedFlag(cliCtx, &dirs)
	if err != nil {
		return
	}
	allPreverified := snapcfg.GetAllCurrentPreverified()
	for chain, webseeds := range snapcfg.KnownWebseeds {
		log.Info("loaded preverified", "chain", chain, "webseeds", webseeds)
		panicif.False(g.MapContains(allPreverified, chain))
	}
	ctx := cliCtx.Context
	httpClient := &http.Client{
		Transport: downloader.MakeWebseedRoundTripper(),
	}
	logger := log.Root()
	checker := webseedChecker{
		ctx:        ctx,
		logger:     logger,
		httpClient: httpClient,
	}
	defer func() {
		logger.Info("finished check",
			"total bytes read", checker.totalBytesRead.Load(),
			"total request count", checker.totalRequestCount.Load())
	}()
	items, ctx := errgroup.WithContext(ctx)
	items.SetLimit(concurrencyFlag.Get(cliCtx))
	var targetChain g.Option[string]
	if cliCtx.IsSet(verifyChainFlag.Name) {
		targetChain.Set(verifyChainFlag.Get(cliCtx))
	}
addItems:
	for chain, webseeds := range snapcfg.KnownWebseeds {
		logger.Debug("maybe skip chain", "target", targetChain, "chain", chain)
		if targetChain.Ok && targetChain.Value != chain {
			continue
		}
		var baseUrl string
		err := errors.New("no valid webseeds")
		for _, webseed := range webseeds {
			baseUrl, err = snapcfg.WebseedToUrl(webseed)
			if err == nil {
				break
			}
		}
		panicif.Err(err)
		preverified := g.MapMustGet(allPreverified, chain)
		panicif.True(preverified.Local)
		for _, item := range preverified.Items {
			if checker.ctx.Err() != nil {
				break addItems
			}
			items.Go(func() (err error) {
				for range 3 {
					if err != nil {
						logger.Warn("retrying failed preverified item check", "baseUrl", baseUrl, "item", item, "err", err)
					}
					var done bool
					done, err = checker.checkPreverifiedItem(baseUrl, item)
					if done || ctx.Err() != nil {
						return err
					}
					panicif.True(err == nil && !done)
				}
				return
			})
		}
	}
	return items.Wait()
}

type webseedChecker struct {
	ctx               context.Context
	logger            log.Logger
	httpClient        *http.Client
	totalBytesRead    atomic.Int64
	totalRequestCount atomic.Int64
}

func (me *webseedChecker) checkPreverifiedItem(baseUrl string, item snapcfg.PreverifiedItem) (done bool, err error) {
	ctx := me.ctx
	httpClient := me.httpClient
	me.logger.Debug("checking preverified item", "webseed", baseUrl, "item", item)
	mi, err := downloader.GetMetainfoFromWebseed(ctx, baseUrl+"/", item.Name, httpClient, io.Discard)
	if err != nil {
		err = fmt.Errorf("getting metainfo from webseed: %w", err)
		return
	}
	me.totalRequestCount.Add(1)
	info, err := mi.UnmarshalInfo()
	panicif.Err(err)
	me.logger.Debug("got metainfo", "piece length", info.PieceLength, "length", info.Length)
	dataUrl := baseUrl + "/" + item.Name
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dataUrl, nil)
	panicif.Err(err)
	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	me.totalRequestCount.Add(1)
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("bad http response status code: %v", resp.StatusCode)
		return
	}
	me.logger.Debug("item response", "etag", resp.Header.Get("etag"), "content length", resp.ContentLength)
	done, err = me.matchHashes(&info, resp)
	if err == nil {
		me.logger.Info("snapshot matches",
			"url", dataUrl,
			//"name", item.Name,
			"content length", resp.ContentLength,
			//"etag", resp.Header.Get("etag"),
		)
	}
	return
}

// Keep this open to multiple bodies, and matching ETags.
func (me *webseedChecker) matchHashes(info *metainfo.Info, resp *http.Response) (
	done bool, // Trying again won't change anything
	err error,
) {
	if resp.ContentLength != -1 {
		panicif.NotEq(resp.ContentLength, info.Length)
	}
	nextHash, stop := iter.Pull(me.yieldHashes(resp.Body, info.PieceLength))
	defer stop()
	for i := range info.NumPieces() {
		p := info.Piece(i)
		hr, ok := nextHash()
		if !ok {
			err = me.ctx.Err()
			if err != nil {
				return
			}
		}
		panicif.False(ok)
		if hr.Err != nil {
			err = fmt.Errorf("getting hash for piece %v: %w", i, hr.Err)
			return
		}
		h := hr.Ok
		panicif.NotEq(h, p.V1Hash().Unwrap())
		log.Debug("matched piece hash", "hash", h, "url", resp.Request.URL)
	}
	// Trying again won't change anything.
	done = true
	panicif.Err(err)
	_, ok := nextHash()
	if ok {
		err = errors.New("response longer than expected")
	}
	return
}

func (me *webseedChecker) yieldHashes(r io.Reader, pieceLength int64) iter.Seq[g.Result[metainfo.Hash]] {
	return func(yield func(g.Result[metainfo.Hash]) bool) {
		h := sha1.New()
		for {
			h.Reset()
			n, err := io.CopyN(h, r, pieceLength)
			me.totalBytesRead.Add(n)
			if err != nil {
				if err != io.EOF {
					yield(result.Err[metainfo.Hash](err))
					return
				}
				if n == 0 {
					return
				}
			}
			var mh metainfo.Hash
			sumRet := h.Sum(mh[:0])
			panicif.NotEq(unsafe.SliceData(mh[:]), unsafe.SliceData(sumRet))
			if !yield(result.Ok(mh)) {
				return
			}
		}

	}
}
