package webseeds

import (
	"cmp"
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"net/http"
	"os"
	"slices"
	"sync/atomic"
	"unsafe"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/generics/result"
	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/snapcfg"
)

func Verify(
	ctx context.Context,
	// local, embedded, preverified etc. Perhaps this should be done exclusively by callers.
	preverifiedFlagValue,
	// If empty, no datadir. This may be okay depending on what preverified set is used.
	dataDir string,
	concurrency int,
	targetChain g.Option[string],
) (err error) {
	// Causes stalls.
	panicif.Zero(concurrency)
	var dirs datadir.Dirs
	if dataDir != "" {
		dirs = datadir.Open(dataDir)
	}
	err = snapcfg.LoadPreverified(ctx, preverifiedFlagValue, &dirs)
	if err != nil {
		return
	}
	allPreverified := snapcfg.GetAllCurrentPreverified()
	chains := selectChains(targetChain, snapcfg.KnownWebseeds)
	if len(chains) == 0 {
		err = errors.New("no matching chains")
		return
	}
	log.Info("selected chains", "chains", chains)
	httpClient := &http.Client{
		Transport: downloader.MakeWebseedRoundTripper(),
	}
	logger := log.Root()
	items, ctx := errgroup.WithContext(ctx)
	items.SetLimit(concurrency)
	checker := webseedChecker{
		ctx:        ctx,
		logger:     logger,
		httpClient: httpClient,
	}
	g.MakeMapWithCap(&checker.state, len(chains))
	defer func() {
		items.Wait()
		// Strict evaluation for the win.
		err = cmp.Or(err, json.NewEncoder(os.Stdout).Encode(checker.state))
		logger.Info("finished check",
			"total bytes read", checker.totalBytesRead.Load(),
			"total request count", checker.totalRequestCount.Load())
	}()
	for _, chain := range chains {
		// Shift left?
		//
		webseeds := g.MapMustGet(snapcfg.KnownWebseeds, chain)
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
		//
		// end shift left?
		err = checker.submitChain(chain, baseUrl, preverified.Items, items)
		if err != nil {
			return fmt.Errorf("while submitting chain %q for verification: %w", chain, err)
		}
	}
	return items.Wait()
}

// Also choose a valid webseed to shift left?
func selectChains[V any](target g.Option[string], known map[string]V) []string {
	if !target.Ok {
		return slices.Collect(maps.Keys(known))
	}
	if g.MapContains(known, target.Value) {
		return []string{target.Value}
	}
	return nil
}

type webseedChecker struct {
	ctx               context.Context
	logger            log.Logger
	httpClient        *http.Client
	totalBytesRead    atomic.Int64
	totalRequestCount atomic.Int64
	state             state
}

// itemState is a pointer so it can be owned by each worker.
type state map[string]map[string]*itemState

type itemState struct {
	ExpectedInfoHash   string
	ActualInfoHash     string
	CorrectInfoHash    bool
	DataMatchesTorrent bool
	Err                string `json:",omitempty"`
	Length             int64
	Etag               string
}

func (checker *webseedChecker) submitChain(
	chain string,
	baseUrl string,
	items []preverified.Item,
	group *errgroup.Group,
) error {
	ctx := checker.ctx
	chainState := make(map[string]*itemState, len(items))
	g.MapMustAssignNew(checker.state, chain, chainState)
	for _, item := range items {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		state := itemState{
			// This needs to always be lowercase hex string?
			ExpectedInfoHash: item.Hash,
		}
		g.MapMustAssignNew(chainState, item.Name, &state)
		group.Go(func() (err error) {
			defer func() {
				if err != nil {
					state.Err = err.Error()
				}
			}()
			for range 3 {
				if err != nil {
					checker.logger.Warn("retrying failed preverified item check", "baseUrl", baseUrl, "item", item, "err", err)
				}
				var done bool
				done, err = checker.checkPreverifiedItem(baseUrl, item, &state)
				if done || ctx.Err() != nil {
					break
				}
				panicif.True(err == nil && !done)
			}
			return
		})
	}
	return nil
}

func (me *webseedChecker) checkPreverifiedItem(
	baseUrl string,
	item snapcfg.PreverifiedItem,
	stateItem *itemState,
) (done bool, err error) {
	ctx := me.ctx
	httpClient := me.httpClient
	me.logger.Debug("checking preverified item", "webseed", baseUrl, "item", item)
	mi, err := downloader.GetMetainfoFromWebseed(ctx, baseUrl+"/", item.Name, httpClient, io.Discard)
	if err != nil {
		err = fmt.Errorf("getting metainfo from webseed: %w", err)
		return
	}
	me.totalRequestCount.Add(1)
	actualInfoHash := mi.HashInfoBytes().HexString()
	stateItem.ActualInfoHash = actualInfoHash
	stateItem.CorrectInfoHash = stateItem.ExpectedInfoHash == actualInfoHash
	if !stateItem.CorrectInfoHash {
		me.logger.Warn("wrong infohash")
	}
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
	etag := resp.Header.Get("ETag")
	stateItem.Etag = etag
	me.logger.Debug("item response", "etag", etag, "content length", resp.ContentLength)
	done, err = me.matchHashes(&info, resp, stateItem)
	if err == nil {
		stateItem.DataMatchesTorrent = true
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
func (me *webseedChecker) matchHashes(info *metainfo.Info, resp *http.Response, stateItem *itemState) (
	done bool, // Trying again won't change anything
	err error,
) {
	if resp.ContentLength != -1 {
		panicif.NotEq(resp.ContentLength, info.Length)
	}
	stateItem.Length = 0
	nextHash, stop := iter.Pull(me.yieldHashes(
		resp.Body,
		info.PieceLength,
		func(n int64) {
			me.totalBytesRead.Add(n)
			stateItem.Length += n
		},
	))
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

func (me *webseedChecker) yieldHashes(r io.Reader, pieceLength int64, onReadN func(n int64)) iter.Seq[g.Result[metainfo.Hash]] {
	return func(yield func(g.Result[metainfo.Hash]) bool) {
		h := sha1.New()
		for {
			h.Reset()
			n, err := io.CopyN(h, r, pieceLength)
			onReadN(n)
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
