// Copyright 2026 The Erigon Authors
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

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

const maxBuilderConfigRequestSize = 2 << 20

type gloasBlockProductionOptions struct {
	builderConfig      *cltypes.BuilderConfig
	includePayload     bool
	suppliedBid        *cltypes.SignedExecutionPayloadBid
	selectedBuilderURL string
}

type gloasBlockProductionOptionsKey struct{}

func decodeGloasBlockProductionOptions(w http.ResponseWriter, r *http.Request, targetSlot uint64) (*gloasBlockProductionOptions, error) {
	version := r.Header.Get("Eth-Consensus-Version")
	if version == "" {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Eth-Consensus-Version header is required"))
	}
	parsedVersion, err := clparams.StringToClVersion(version)
	if err != nil || parsedVersion != clparams.GloasVersion {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("v4 block production requires gloas consensus version"))
	}
	includePayloadValue := r.URL.Query().Get("include_payload")
	if includePayloadValue == "" {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("include_payload query parameter is required"))
	}
	includePayload, err := strconv.ParseBool(includePayloadValue)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid include_payload: %w", err))
	}
	config := new(cltypes.BuilderConfig)
	contentType, err := requestContentType(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, err)
	}
	switch contentType {
	case "application/json":
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBuilderConfigRequestSize))
		if err := decoder.Decode(config); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("request body contains trailing data"))
		}
	case "application/octet-stream":
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBuilderConfigRequestSize))
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if err := config.DecodeSSZStrict(body, int(clparams.GloasVersion)); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
	default:
		return nil, beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, fmt.Errorf("unsupported content type: %s", contentType))
	}
	for i, entry := range config.Builders {
		if entry == nil || entry.Auth == nil || entry.Auth.Message == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("builder %d has invalid auth", i))
		}
		if entry.Auth.Message.Slot != targetSlot {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("builder %d auth slot %d does not match proposal slot %d", i, entry.Auth.Message.Slot, targetSlot))
		}
	}
	return &gloasBlockProductionOptions{builderConfig: config, includePayload: includePayload}, nil
}

func (a *ApiHandler) PostEthV4ValidatorBlock(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	targetSlot, err := strconv.ParseUint(chi.URLParam(r, "slot"), 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid slot: %w", err))
	}
	if a.beaconChainCfg.SlotsPerEpoch == 0 || targetSlot/a.beaconChainCfg.SlotsPerEpoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("v4 block production is unavailable before Gloas"))
	}
	options, err := decodeGloasBlockProductionOptions(w, r, targetSlot)
	if err != nil {
		return nil, err
	}
	r = r.WithContext(context.WithValue(r.Context(), gloasBlockProductionOptionsKey{}, options))
	return a.GetEthV3ValidatorBlock(w, r)
}

func (a *ApiHandler) PostEthV4ValidatorBlockWithBid(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	targetSlot, err := strconv.ParseUint(chi.URLParam(r, "slot"), 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid slot: %w", err))
	}
	if a.beaconChainCfg.SlotsPerEpoch == 0 || targetSlot/a.beaconChainCfg.SlotsPerEpoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("v4 block production is unavailable before Gloas"))
	}
	if r.Header.Get("Eth-Consensus-Version") != clparams.GloasVersion.String() {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Gloas Eth-Consensus-Version header is required"))
	}
	includePayload := false
	if value := r.URL.Query().Get("include_payload"); value != "" {
		includePayload, err = strconv.ParseBool(value)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid include_payload: %w", err))
		}
	}
	boost := uint64(100)
	if value := r.URL.Query().Get("builder_boost_factor"); value != "" {
		boost, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid builder_boost_factor: %w", err))
		}
	}
	bid := new(cltypes.SignedExecutionPayloadBid)
	contentType, err := requestContentType(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, err)
	}
	switch contentType {
	case "application/json":
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEpbsJSONSize))
		if err := decoder.Decode(bid); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("request body contains trailing data"))
		}
	case "application/octet-stream":
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSignedExecutionPayloadBidSSZSize()))
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		if err := bid.DecodeSSZ(body, int(clparams.GloasVersion)); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
	default:
		return nil, beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, fmt.Errorf("unsupported content type: %s", contentType))
	}
	if bid.Message == nil || bid.Message.Slot != targetSlot {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("execution payload bid slot does not match proposal slot"))
	}
	options := &gloasBlockProductionOptions{
		builderConfig:  &cltypes.BuilderConfig{BuilderBoostFactor: boost},
		includePayload: includePayload,
		suppliedBid:    bid,
	}
	r = r.WithContext(context.WithValue(r.Context(), gloasBlockProductionOptionsKey{}, options))
	return a.GetEthV3ValidatorBlock(w, r)
}

func gloasBlockOptionsFromContext(ctx context.Context) *gloasBlockProductionOptions {
	options, _ := ctx.Value(gloasBlockProductionOptionsKey{}).(*gloasBlockProductionOptions)
	return options
}
