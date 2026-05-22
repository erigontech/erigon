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

package handshake

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sync"

	communication2 "github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/libp2p/go-libp2p/core/peer"
)

// HandShaker is the data type which will handle handshakes and determine if
// The peer is worth connecting to or not.
type HandShaker struct {
	ctx context.Context
	// Status object to send over.
	status             *cltypes.Status // Contains status object for handshakes
	set                bool
	handler            http.Handler
	beaconConfig       *clparams.BeaconChainConfig
	ethClock           eth_clock.EthereumClock
	peerDasStateReader peerdasstate.PeerDasStateReader

	mu sync.Mutex
}

func New(ctx context.Context, ethClock eth_clock.EthereumClock, beaconConfig *clparams.BeaconChainConfig, handler http.Handler, peerDasStateReader peerdasstate.PeerDasStateReader) *HandShaker {
	// Compute initial fork digest so the status message is never all-zeros.
	// Without this, peers connecting before SetStatus is called see a genesis-like
	// status and may request blocks we cannot serve.
	initialStatus := &cltypes.Status{}
	if forkDigest, err := ethClock.CurrentForkDigest(); err == nil {
		initialStatus.ForkDigest = forkDigest
	}
	return &HandShaker{
		ctx:                ctx,
		handler:            handler,
		ethClock:           ethClock,
		beaconConfig:       beaconConfig,
		status:             initialStatus,
		peerDasStateReader: peerDasStateReader,
	}
}

// SetStatus sets the current network status against which we can validate peers.
func (h *HandShaker) SetStatus(status *cltypes.Status) {
	if status == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.set = true

	if status.ForkDigest != [4]byte{} {
		h.status.ForkDigest = status.ForkDigest
	}

	if status.FinalizedRoot != [32]byte{} {
		h.status.FinalizedRoot = status.FinalizedRoot
	}

	// Always update FinalizedEpoch and HeadSlot — 0 is a valid value at genesis.
	// Skipping 0 prevents the status from being properly set at genesis, causing
	// peers to see a stale head and ban us as "useless".
	h.status.FinalizedEpoch = status.FinalizedEpoch
	h.status.HeadSlot = status.HeadSlot

	if status.HeadRoot != [32]byte{} {
		h.status.HeadRoot = status.HeadRoot
	}

	if status.EarliestAvailableSlot != nil {
		h.status.EarliestAvailableSlot = status.EarliestAvailableSlot
	}
}

// Status returns the underlying status (only for giving out responses)
func (h *HandShaker) Status() *cltypes.Status {
	h.mu.Lock()
	defer h.mu.Unlock()

	// copy the status and add the earliest available slot
	status := *h.status
	if curEpoch := h.ethClock.GetCurrentEpoch(); curEpoch >= h.beaconConfig.FuluForkEpoch {
		// get earliest available slot after fulu fork
		earliestAvailableSlot := h.peerDasStateReader.GetEarliestAvailableSlot()
		status.EarliestAvailableSlot = &earliestAvailableSlot
	}
	return &status
}

// Set returns the underlying status (only for giving out responses)
func (h *HandShaker) IsSet() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.set
}

func (h *HandShaker) ValidatePeer(id peer.ID) (bool, error) {
	// Always validate fork digest — the constructor initialises it from
	// ethClock, so we can reject wrong-network peers even before the CL
	// stages call SetStatus with finalized/head info.
	status := h.Status()
	topic := communication2.StatusProtocolV1
	fallbackBodyHex := "" // hex-encoded v1 body for when peer negotiates v1

	// For Fulu+, prefer v2 with v1 fallback. The primary body (r.Body) is
	// v2-format (92 bytes SSZ); the fallback body (hex header) is v1-format
	// (84 bytes SSZ). server.go selects the right body after negotiation.
	if curEpoch := h.ethClock.GetCurrentEpoch(); curEpoch >= h.beaconConfig.FuluForkEpoch {
		// Build v1 fallback body (no EarliestAvailableSlot)
		v1Status := *status
		v1Status.EarliestAvailableSlot = nil
		v1Buf := new(bytes.Buffer)
		if err := ssz_snappy.EncodeAndWrite(v1Buf, &v1Status); err != nil {
			return false, err
		}
		fallbackBodyHex = hex.EncodeToString(v1Buf.Bytes())
		topic = communication2.StatusProtocolV2 + "," + communication2.StatusProtocolV1
	} else {
		// Pre-Fulu: strip EarliestAvailableSlot for v1-only
		status.EarliestAvailableSlot = nil
	}

	buf := new(bytes.Buffer)
	if err := ssz_snappy.EncodeAndWrite(buf, status); err != nil {
		return false, err
	}
	req, err := http.NewRequest("GET", "http://service.internal/", buf)
	if err != nil {
		return false, err
	}
	req.Header.Set("REQRESP-PEER-ID", id.String())
	req.Header.Set("REQRESP-TOPIC", topic)
	if fallbackBodyHex != "" {
		req.Header.Set("REQRESP-FALLBACK-BODY", fallbackBodyHex)
	}
	resp, err := httpreqresp.Do(h.handler, req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.Header.Get("REQRESP-RESPONSE-CODE") != "0" {
		a, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("hand shake error: %s, %s", resp.Header.Get("REQRESP-RESPONSE-CODE"), string(a))
	}
	responseStatus := &cltypes.Status{}

	// Use the negotiated protocol to pick the right SSZ version for decoding.
	respVersion := clparams.Phase0Version
	if resp.Header.Get("REQRESP-TOPIC") == communication2.StatusProtocolV2 {
		respVersion = clparams.FuluVersion
	}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(resp.Body, responseStatus, respVersion); err != nil {
		log.Debug("DecodeAndReadNoForkDigest", "error", err)
		return false, nil
	}
	forkDigest, err := h.ethClock.CurrentForkDigest()
	if err != nil {
		return false, err
	}
	if responseStatus.ForkDigest != forkDigest {
		respDigest := common.Bytes4{}
		copy(respDigest[:], responseStatus.ForkDigest[:])
		log.Trace("Fork digest mismatch", "responseStatus.ForkDigest", respDigest, "forkDigest", forkDigest, "responseStatus.HeadSlot", responseStatus.HeadSlot)
		return false, nil
	}
	return true, nil
}
