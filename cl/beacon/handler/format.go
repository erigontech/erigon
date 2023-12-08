package handler

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/go-chi/chi/v5"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
)

type apiError struct {
	code int
	err  error
}

type beaconResponse struct {
	Data                any                    `json:"data,omitempty"`
	Finalized           *bool                  `json:"finalized,omitempty"`
	Version             *clparams.StateVersion `json:"version,omitempty"`
	ExecutionOptimistic *bool                  `json:"execution_optimistic,omitempty"`
}

func (b *beaconResponse) EncodeSSZ(xs []byte) ([]byte, error) {
	marshaler, ok := b.Data.(ssz.Marshaler)
	if !ok {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, "This endpoint does not support SSZ response")
	}
	encoded, err := marshaler.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func (b *beaconResponse) EncodingSizeSSZ() int {
	marshaler, ok := b.Data.(ssz.Marshaler)
	if !ok {
		return 9
	}
	return marshaler.EncodingSizeSSZ()
}

func newBeaconResponse(data any) *beaconResponse {
	return &beaconResponse{
		Data: data,
	}
}

func (r *beaconResponse) withFinalized(finalized bool) (out *beaconResponse) {
	out = new(beaconResponse)
	*out = *r
	out.Finalized = new(bool)
	out.ExecutionOptimistic = new(bool)
	out.Finalized = &finalized
	return r
}

func (r *beaconResponse) withVersion(version clparams.StateVersion) (out *beaconResponse) {
	out = new(beaconResponse)
	*out = *r
	out.Version = new(clparams.StateVersion)
	out.Version = &version
	return r
}

//// In case of it being a json we need to also expose finalization, version, etc...
//type beaconHandlerFn func(r *http.Request) *beaconResponse
//
//func beaconHandlerWrapper(fn beaconHandlerFn, supportSSZ bool) func(w http.ResponseWriter, r *http.Request) {
//	return func(w http.ResponseWriter, r *http.Request) {
//		accept := r.Header.Get("Accept")
//		isSSZ := !strings.Contains(accept, "application/json") && strings.Contains(accept, "application/stream-octect")
//		start := time.Now()
//		defer func() {
//			log.Debug("[Beacon API] finished", "method", r.Method, "path", r.URL.Path, "duration", time.Since(start))
//		}()
//
//		resp := fn(r)
//		if resp.internalError != nil {
//			http.Error(w, resp.internalError.Error(), http.StatusInternalServerError)
//			log.Debug("[Beacon API] failed", "method", r.Method, "err", resp.internalError.Error(), "ssz", isSSZ)
//			return
//		}
//
//		if resp.apiError != nil {
//			http.Error(w, resp.apiError.err.Error(), resp.apiError.code)
//			log.Debug("[Beacon API] failed", "method", r.Method, "err", resp.apiError.err.Error(), "ssz", isSSZ)
//			return
//		}
//
//		if isSSZ && supportSSZ {
//			data := resp.Data
//			// SSZ encoding
//			encoded, err := data.(ssz.Marshaler).EncodeSSZ(nil)
//			if err != nil {
//				http.Error(w, err.Error(), http.StatusInternalServerError)
//				log.Debug("[Beacon API] failed", "method", r.Method, "err", err, "accepted", accept)
//				return
//			}
//			w.Header().Set("Content-Type", "application/octet-stream")
//			w.Write(encoded)
//			return
//		}
//		w.Header().Set("Content-Type", "application/json")
//		if err := json.NewEncoder(w).Encode(resp); err != nil {
//			log.Warn("[Beacon API] failed", "method", r.Method, "err", err, "ssz", isSSZ)
//		}
//	}
//}

type chainTag int

var (
	Head      chainTag = 0
	Finalized chainTag = 1
	Justified chainTag = 2
	Genesis   chainTag = 3
)

// Represent either state id or block id
type segmentID struct {
	tag  chainTag
	slot *uint64
	root *libcommon.Hash
}

func (c *segmentID) head() bool {
	return c.tag == Head && c.slot == nil && c.root == nil
}

func (c *segmentID) finalized() bool {
	return c.tag == Finalized
}

func (c *segmentID) justified() bool {
	return c.tag == Justified
}

func (c *segmentID) genesis() bool {
	return c.tag == Genesis
}

func (c *segmentID) getSlot() *uint64 {
	return c.slot
}

func (c *segmentID) getRoot() *libcommon.Hash {
	return c.root
}

func epochFromRequest(r *http.Request) (uint64, error) {
	// Must only be a number
	regex := regexp.MustCompile(`^\d+$`)
	epoch := chi.URLParam(r, "epoch")
	if !regex.MatchString(epoch) {
		return 0, fmt.Errorf("invalid path variable: {epoch}")
	}
	epochMaybe, err := strconv.ParseUint(epoch, 10, 64)
	if err != nil {
		return 0, err
	}
	return epochMaybe, nil
}

func blockIdFromRequest(r *http.Request) (*segmentID, error) {
	regex := regexp.MustCompile(`^(?:0x[0-9a-fA-F]{64}|head|finalized|genesis|\d+)$`)

	blockId := chi.URLParam(r, "block_id")
	if !regex.MatchString(blockId) {
		return nil, fmt.Errorf("invalid path variable: {block_id}")
	}

	if blockId == "head" {
		return &segmentID{tag: Head}, nil
	}
	if blockId == "finalized" {
		return &segmentID{tag: Finalized}, nil
	}
	if blockId == "genesis" {
		return &segmentID{tag: Genesis}, nil
	}
	slotMaybe, err := strconv.ParseUint(blockId, 10, 64)
	if err == nil {
		return &segmentID{slot: &slotMaybe}, nil
	}
	root := libcommon.HexToHash(blockId)
	return &segmentID{
		root: &root,
	}, nil
}

func stateIdFromRequest(r *http.Request) (*segmentID, error) {
	regex := regexp.MustCompile(`^(?:0x[0-9a-fA-F]{64}|head|finalized|genesis|justified|\d+)$`)

	stateId := chi.URLParam(r, "state_id")
	if !regex.MatchString(stateId) {
		return nil, fmt.Errorf("invalid path variable: {block_id}")
	}

	if stateId == "head" {
		return &segmentID{tag: Head}, nil
	}
	if stateId == "finalized" {
		return &segmentID{tag: Finalized}, nil
	}
	if stateId == "genesis" {
		return &segmentID{tag: Genesis}, nil
	}
	if stateId == "justified" {
		return &segmentID{tag: Justified}, nil
	}
	slotMaybe, err := strconv.ParseUint(stateId, 10, 64)
	if err == nil {
		return &segmentID{slot: &slotMaybe}, nil
	}
	root := libcommon.HexToHash(stateId)
	return &segmentID{
		root: &root,
	}, nil
}

func hashFromQueryParams(r *http.Request, name string) (*libcommon.Hash, error) {
	hashStr := r.URL.Query().Get(name)
	if hashStr == "" {
		return nil, nil
	}
	// check if hashstr is an hex string
	if len(hashStr) != 2+2*32 {
		return nil, fmt.Errorf("invalid hash length")
	}
	if hashStr[:2] != "0x" {
		return nil, fmt.Errorf("invalid hash prefix")
	}
	notHex, err := regexp.MatchString("[^0-9A-Fa-f]", hashStr[2:])
	if err != nil {
		return nil, err
	}
	if notHex {
		return nil, fmt.Errorf("invalid hash characters")
	}

	hash := libcommon.HexToHash(hashStr)
	return &hash, nil
}

// uint64FromQueryParams retrieves a number from the query params, in base 10.
func uint64FromQueryParams(r *http.Request, name string) (*uint64, error) {
	str := r.URL.Query().Get(name)
	if str == "" {
		return nil, nil
	}
	num, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return nil, err
	}
	return &num, nil
}
