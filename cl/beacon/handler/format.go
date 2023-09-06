package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
)

type BeaconResponse struct {
	Finalized           *bool  `json:"finalized,omitempty"`
	Version             string `json:"version,omitempty"`
	ExecutionOptimistic *bool  `json:"execution_optimistic,omitempty"`
	Data                any    `json:"data,omitempty"`
}

// In case of it being a json we need to also expose finalization, version, etc...
type beaconHandlerFn func(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error)

func beaconHandlerWrapper(fn beaconHandlerFn) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		isSSZ := !strings.Contains(accept, "application/json") && strings.Contains(accept, "application/stream-octect")
		data, finalized, version, httpStatus, err := fn(r)
		if err != nil {
			w.WriteHeader(httpStatus)
			io.WriteString(w, err.Error())
			log.Debug("[Beacon API] failed", "method", r.Method, "err", err, "ssz", isSSZ)
			return
		}

		if isSSZ {
			// SSZ encoding
			encoded, err := data.(ssz.Marshaler).EncodeSSZ(nil)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, err.Error())
				log.Debug("[Beacon API] failed", "method", r.Method, "err", err, "accepted", accept)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(httpStatus)
			w.Write(encoded)
			log.Debug("[Beacon API] genesis handler failed", err)
			return
		}
		resp := &BeaconResponse{Data: data}
		if version != nil {
			resp.Version = clparams.ClVersionToString(*version)
		}
		if finalized != nil {
			resp.ExecutionOptimistic = new(bool)
			resp.Finalized = new(bool)
			*resp.Finalized = *finalized
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		json.NewEncoder(w).Encode(resp)
	}
}

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
