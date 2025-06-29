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
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	communication2 "github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/cl/utils/eth_clock"

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
	return &HandShaker{
		ctx:                ctx,
		handler:            handler,
		ethClock:           ethClock,
		beaconConfig:       beaconConfig,
		status:             &cltypes.Status{},
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

	if status.FinalizedEpoch != 0 {
		h.status.FinalizedEpoch = status.FinalizedEpoch
	}

	if status.HeadRoot != [32]byte{} {
		h.status.HeadRoot = status.HeadRoot
	}

	if status.HeadSlot != 0 {
		h.status.HeadSlot = status.HeadSlot
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
	// Unprotected if it is not set
	if !h.IsSet() {
		return true, nil
	}
	status := h.Status()
	topic := communication2.StatusProtocolV1
	if curEpoch := h.ethClock.GetCurrentEpoch(); curEpoch >= h.beaconConfig.FuluForkEpoch {
		topic = communication2.StatusProtocolV2
	}
	// Encode our status
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
	resp, err := httpreqresp.Do(h.handler, req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.Header.Get("REQRESP-RESPONSE-CODE") != "0" {
		a, _ := io.ReadAll(resp.Body)
		//TODO: proper errors
		return false, fmt.Errorf("hand shake error: %s, %s", resp.Header.Get("REQRESP-RESPONSE-CODE"), string(a))
	}
	responseStatus := &cltypes.Status{}

	if err := ssz_snappy.DecodeAndReadNoForkDigest(resp.Body, responseStatus, clparams.Phase0Version); err != nil {
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
