package handshake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	communication2 "github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/sentinel/httpreqresp"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/libp2p/go-libp2p/core/peer"
)

// HandShaker is the data type which will handle handshakes and determine if
// The peer is worth connecting to or not.
type HandShaker struct {
	ctx context.Context
	// Status object to send over.
	status        *cltypes.Status // Contains status object for handshakes
	set           bool
	handler       http.Handler
	genesisConfig *clparams.GenesisConfig
	beaconConfig  *clparams.BeaconChainConfig

	mu sync.Mutex
}

func New(ctx context.Context, genesisConfig *clparams.GenesisConfig, beaconConfig *clparams.BeaconChainConfig, handler http.Handler) *HandShaker {
	return &HandShaker{
		ctx:           ctx,
		handler:       handler,
		genesisConfig: genesisConfig,
		beaconConfig:  beaconConfig,
		status:        &cltypes.Status{},
	}
}

// SetStatus sets the current network status against which we can validate peers.
func (h *HandShaker) SetStatus(status *cltypes.Status) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.set = true
	h.status = status
}

// Status returns the underlying status (only for giving out responses)
func (h *HandShaker) Status() *cltypes.Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status
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
	req.Header.Set("REQRESP-TOPIC", communication2.StatusProtocolV1)
	resp, err := httpreqresp.Do(h.handler, req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.Header.Get("REQRESP-RESPONSE-CODE") != "0" {
		a, _ := io.ReadAll(resp.Body)
		//TODO: proper errors
		return false, fmt.Errorf("handshake error: %s", string(a))
	}
	responseStatus := &cltypes.Status{}

	if err := ssz_snappy.DecodeAndReadNoForkDigest(resp.Body, responseStatus, clparams.Phase0Version); err != nil {
		return false, nil
	}
	forkDigest, err := fork.ComputeForkDigest(h.beaconConfig, h.genesisConfig)
	if err != nil {
		return false, nil
	}
	return responseStatus.ForkDigest == forkDigest, nil
}
