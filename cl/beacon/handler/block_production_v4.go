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
		if err := decodeBuilderConfigJSON(decoder, config); err != nil {
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
	validBuilders := make([]*cltypes.BuilderEntry, 0, len(config.Builders))
	for _, entry := range config.Builders {
		if entry == nil || entry.Auth == nil || entry.Auth.Message == nil || entry.Auth.Message.Slot != targetSlot {
			continue
		}
		validBuilders = append(validBuilders, entry)
	}
	config.Builders = validBuilders
	return &gloasBlockProductionOptions{builderConfig: config, includePayload: includePayload}, nil
}

func decodeBuilderConfigJSON(decoder *json.Decoder, config *cltypes.BuilderConfig) error {
	var raw struct {
		MinBid             *uint64            `json:"min_bid,string"`
		BuilderBoostFactor *uint64            `json:"builder_boost_factor,string"`
		Builders           *[]json.RawMessage `json:"builders"`
	}
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if raw.MinBid == nil || raw.BuilderBoostFactor == nil || raw.Builders == nil {
		return errors.New("builder config is missing a required field")
	}
	if len(*raw.Builders) > cltypes.MaxBuilderEntries {
		return fmt.Errorf("builder count %d exceeds %d", len(*raw.Builders), cltypes.MaxBuilderEntries)
	}
	config.MinBid = *raw.MinBid
	config.BuilderBoostFactor = *raw.BuilderBoostFactor
	config.Builders = make([]*cltypes.BuilderEntry, 0, len(*raw.Builders))
	for _, encoded := range *raw.Builders {
		entry := new(cltypes.BuilderEntry)
		if err := json.Unmarshal(encoded, entry); err == nil {
			config.Builders = append(config.Builders, entry)
		}
	}
	return nil
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

func gloasBlockOptionsFromContext(ctx context.Context) *gloasBlockProductionOptions {
	options, _ := ctx.Value(gloasBlockProductionOptionsKey{}).(*gloasBlockProductionOptions)
	return options
}
