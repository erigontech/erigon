package stagedsync

import (
	"context"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/whitelist"
)

const maxSpanPrefetchBufferSize = 32

func NewBordHeimdallPreFetcher(
	startSpanId uint64,
	endSpanId uint64,
	startBlockIdBoundary uint64,
	endBlockIdBoundary uint64,
) *BorHeimdallPreFetcher {
	spanCount := endSpanId - startSpanId + 1
	bufferSize := math.Min(spanCount, uint64(maxSpanPrefetchBufferSize))
	return &BorHeimdallPreFetcher{
		startSpanNum:          startSpanId,
		endSpanNum:            endSpanId,
		startBlockNumBoundary: startBlockIdBoundary,
		endBlockNumBoundary:   endBlockIdBoundary,
		spChans:               make([]chan *BorHeimdallSpanProcessor, bufferSize),
		fetchErrChans:         make([]chan error, bufferSize),
	}
}

type BorHeimdallPreFetcher struct {
	cfg                   BorHeimdallCfg
	logger                log.Logger
	logPrefix             string
	chainHR               consensus.ChainHeaderReader
	whitelist             *whitelist.Service
	unwinder              Unwinder
	mine                  bool
	startSpanNum          uint64
	endSpanNum            uint64
	startBlockNumBoundary uint64
	endBlockNumBoundary   uint64
	spChans               []chan *BorHeimdallSpanProcessor // internal
	fetchErrChans         []chan error                     // internal
	chansIdx              uint64
	spOrderedChan         chan *BorHeimdallSpanProcessor // external
	errChan               chan error                     // external
}

func (bhpf *BorHeimdallPreFetcher) SpanProcessors() chan *BorHeimdallSpanProcessor {
	return bhpf.spOrderedChan
}

func (bhpf *BorHeimdallPreFetcher) Err() chan error {
	return bhpf.errChan
}

func (bhpf *BorHeimdallPreFetcher) PrefetchSpansAsync(ctx context.Context) {
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		// used to cancel all active goroutines that have been spawned upon exit
		defer cancel()

		// fire initial fetches
		for range bhpf.spChans {
			bhpf.newSpanAsyncFetch(ctx)
		}

		// wait for fetch results in order and once the first in order one
		// is ready then schedule the next one
		for spanId := bhpf.startSpanNum; spanId <= bhpf.endSpanNum; spanId++ {
			spChan := bhpf.spChans[bhpf.chansCircularIdx()]
			errChan := bhpf.fetchErrChans[bhpf.chansCircularIdx()]
			select {
			case sp := <-spChan:
				bhpf.sendOrdered(ctx, sp)
				bhpf.newSpanAsyncFetch(ctx)
			case err := <-errChan:
				bhpf.sendErr(ctx, err)
				return
			}
		}
	}()
}

func (bhpf *BorHeimdallPreFetcher) newSpanAsyncFetch(ctx context.Context) {
	svChan := make(chan *BorHeimdallSpanProcessor)
	bhpf.spChans[bhpf.chansIdx] = svChan

	fetchErrChan := make(chan error)
	bhpf.fetchErrChans[bhpf.chansIdx] = fetchErrChan

	sp := NewBorHeimdallSpanProcessor(
		bhpf.cfg,
		bhpf.logger,
		bhpf.logPrefix,
		bhpf.chainHR,
		bhpf.whitelist,
		bhpf.unwinder,
		bhpf.mine,
		bhpf.chansIdx+bhpf.startSpanNum,
		bhpf.startBlockNumBoundary,
		bhpf.endBlockNumBoundary,
	)

	sp.FetchSpanAsync(ctx, svChan, fetchErrChan)
	bhpf.incrementChansIdx()
}

func (bhpf *BorHeimdallPreFetcher) incrementChansIdx() {
	bhpf.chansIdx = (bhpf.chansIdx + 1) % uint64(len(bhpf.spChans))
}

func (bhpf *BorHeimdallPreFetcher) chansCircularIdx() uint64 {
	return bhpf.chansIdx % uint64(len(bhpf.spChans))
}

func (bhpf *BorHeimdallPreFetcher) sendOrdered(
	ctx context.Context,
	sp *BorHeimdallSpanProcessor,
) {
	select {
	case <-ctx.Done():
	case bhpf.spOrderedChan <- sp:
	}
}

func (bhpf *BorHeimdallPreFetcher) sendErr(ctx context.Context, err error) {
	select {
	case <-ctx.Done():
	case bhpf.errChan <- err:
	}
}
