package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

type chainReader struct {
	config *params.ChainConfig
}

func (cr chainReader) Config() *params.ChainConfig                             { return cr.config }
func (cr chainReader) CurrentHeader() *types.Header                            { panic("") }
func (cr chainReader) GetHeader(hash common.Hash, number uint64) *types.Header { panic("") }
func (cr chainReader) GetHeaderByNumber(number uint64) *types.Header           { panic("") }
func (cr chainReader) GetHeaderByHash(hash common.Hash) *types.Header          { panic("") }

func processSegment(hd *headerdownload.HeaderDownload, segment *headerdownload.ChainSegment) {
	log.Info(hd.AnchorState())
	log.Info("processSegment", "from", segment.Headers[0].Number.Uint64(), "to", segment.Headers[len(segment.Headers)-1].Number.Uint64())
	foundAnchor, start, anchorParent, invalidAnchors := hd.FindAnchors(segment)
	if len(invalidAnchors) > 0 {
		if _, err1 := hd.InvalidateAnchors(anchorParent, invalidAnchors); err1 != nil {
			log.Error("Invalidation of anchor failed", "error", err1)
		}
		log.Warn(fmt.Sprintf("Invalidated anchors %v for %x", invalidAnchors, anchorParent))
	}
	foundTip, end, penalty := hd.FindTip(segment, start) // We ignore penalty because we will check it as part of PoW check
	if penalty != headerdownload.NoPenalty {
		log.Error(fmt.Sprintf("FindTip penalty %d", penalty))
		return
	}
	currentTime := uint64(time.Now().Unix())
	var powDepth int
	if powDepth1, err1 := hd.VerifySeals(segment, foundAnchor, foundTip, start, end, currentTime); err1 == nil {
		powDepth = powDepth1
	} else {
		log.Error("VerifySeals", "error", err1)
		return
	}
	if err1 := hd.FlushBuffer(); err1 != nil {
		log.Error("Could not flush the buffer, will discard the data", "error", err1)
		return
	}
	// There are 4 cases
	if foundAnchor {
		if foundTip {
			// Connect
			if err1 := hd.Connect(segment, start, end, currentTime); err1 != nil {
				log.Error("Connect failed", "error", err1)
			} else {
				hd.AddSegmentToBuffer(segment, start, end)
				log.Info("Connected", "start", start, "end", end)
			}
		} else {
			// ExtendDown
			if err1 := hd.ExtendDown(segment, start, end, powDepth, currentTime); err1 != nil {
				log.Error("ExtendDown failed", "error", err1)
			} else {
				hd.AddSegmentToBuffer(segment, start, end)
				log.Info("Extended Down", "start", start, "end", end)
			}
		}
	} else if foundTip {
		if end == 0 {
			log.Info("No action needed, tip already exists")
		} else {
			// ExtendUp
			if err1 := hd.ExtendUp(segment, start, end, currentTime); err1 != nil {
				log.Error("ExtendUp failed", "error", err1)
			} else {
				hd.AddSegmentToBuffer(segment, start, end)
				log.Info("Extended Up", "start", start, "end", end)
			}
		}
	} else {
		// NewAnchor
		if err1 := hd.NewAnchor(segment, start, end, currentTime); err1 != nil {
			log.Error("NewAnchor failed", "error", err1)
		} else {
			hd.AddSegmentToBuffer(segment, start, end)
			log.Info("NewAnchor", "start", start, "end", end)
		}
	}
	if start == 0 || end > 0 {
		hd.CheckInitiation(segment, params.MainnetGenesisHash)
	}
}

// Downloader needs to be run from a go-routine, and it is in the sole control of the HeaderDownloader object
func Downloader(
	ctx context.Context,
	filesDir string,
	bufferLimit int,
	newBlockCh chan NewBlockFromSentry,
	newBlockHashCh chan NewBlockHashFromSentry,
	headersCh chan BlockHeadersFromSentry,
	penaltyCh chan PenaltyMsg,
	reqHeadersCh chan headerdownload.HeaderRequest,
) {
	//config := eth.DefaultConfig.Ethash
	engine := ethash.New(ethash.Config{
		CachesInMem:      1,
		CachesLockMmap:   false,
		DatasetDir:       "ethash",
		DatasetsInMem:    1,
		DatasetsOnDisk:   0,
		DatasetsLockMmap: false,
	}, nil, false)
	cr := chainReader{config: params.MainnetChainConfig}
	calcDiffFunc := func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		return engine.CalcDifficulty(cr, childTimestamp, parentTime, parentDifficulty, parentNumber, parentHash, parentUncleHash)
	}
	verifySealFunc := func(header *types.Header) error {
		return engine.VerifySeal(cr, header)
	}
	hd := headerdownload.NewHeaderDownload(
		filesDir,
		bufferLimit, /* bufferLimit */
		16*1024,     /* tipLimit */
		1024,        /* initPowDepth */
		calcDiffFunc,
		verifySealFunc,
		3600, /* newAnchor future limit */
		3600, /* newAnchor past limit */
	)
	hd.InitHardCodedTips("hard-coded-headers.dat")
	if recovered, err := hd.RecoverFromFiles(uint64(time.Now().Unix())); err != nil || !recovered {
		if err != nil {
			log.Error("Recovery from file failed, will start from scratch", "error", err)
		}
		// Insert hard-coded headers if present
		if _, err := os.Stat("hard-coded-headers.dat"); err == nil {
			if f, err1 := os.Open("hard-coded-headers.dat"); err1 == nil {
				var hBuffer [headerdownload.HeaderSerLength]byte
				i := 0
				for {
					var h types.Header
					if _, err2 := io.ReadFull(f, hBuffer[:]); err2 == nil {
						headerdownload.DeserialiseHeader(&h, hBuffer[:])
					} else if errors.Is(err2, io.EOF) {
						break
					} else {
						log.Error("Failed to read hard coded header", "i", i, "error", err2)
						break
					}
					if err2 := hd.HardCodedHeader(&h, uint64(time.Now().Unix())); err2 != nil {
						log.Error("Failed to insert hard coded header", "i", i, "block", h.Number.Uint64(), "error", err2)
					} else {
						hd.AddHeaderToBuffer(&h)
					}
					i++
				}
			}
		}
	}
	log.Info(hd.AnchorState())
	for {
		select {
		case newBlockReq := <-newBlockCh:
			if segments, penalty, err := hd.SingleHeaderAsSegment(newBlockReq.Block.Header()); err == nil {
				if penalty == headerdownload.NoPenalty {
					processSegment(hd, segments[0]) // There is only one segment in this case
				} else {
					// Send penalty back to the sentry
					penaltyCh <- PenaltyMsg{SentryMsg: newBlockReq.SentryMsg, penalty: penalty}
				}
			} else {
				log.Error("SingleHeaderAsSegment failed", "error", err)
				continue
			}
			log.Info(fmt.Sprintf("NewBlockMsg{blockNumber: %d}", newBlockReq.Block.NumberU64()))
		case newBlockHashReq := <-newBlockHashCh:
			for _, announce := range newBlockHashReq.NewBlockHashesData {
				if !hd.HasTip(announce.Hash) {
					log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", announce.Hash, announce.Number, 1))
					reqHeadersCh <- headerdownload.HeaderRequest{
						Hash:   announce.Hash,
						Number: announce.Number,
						Length: 1,
					}
				}
			}
		case headersReq := <-headersCh:
			if segments, penalty, err := hd.SplitIntoSegments(headersReq.headers); err == nil {
				if penalty == headerdownload.NoPenalty {
					for _, segment := range segments {
						processSegment(hd, segment)
					}
				} else {
					penaltyCh <- PenaltyMsg{SentryMsg: headersReq.SentryMsg, penalty: penalty}
				}
			} else {
				log.Error("SingleHeaderAsSegment failed", "error", err)
			}
			log.Info("HeadersMsg processed")
		case <-hd.RequestQueueTimer.C:
			fmt.Printf("RequestQueueTimer ticked\n")
		case <-ctx.Done():
			return
		}
		reqs := hd.RequestMoreHeaders(uint64(time.Now().Unix()), 5 /*timeout */)
		for _, req := range reqs {
			//log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", req.Hash, req.Number, req.Length))
			reqHeadersCh <- *req
		}
	}
}
