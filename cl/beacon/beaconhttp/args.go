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

package beaconhttp

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/erigontech/erigon-lib/common"
)

type chainTag int

var (
	Head      chainTag = 0
	Finalized chainTag = 1
	Justified chainTag = 2
	Genesis   chainTag = 3
)

// Represent either state id or block id
type SegmentID struct {
	tag  chainTag
	slot *uint64
	root *common.Hash
}

func (c *SegmentID) Head() bool {
	return c.tag == Head && c.slot == nil && c.root == nil
}

func (c *SegmentID) Finalized() bool {
	return c.tag == Finalized
}

func (c *SegmentID) Justified() bool {
	return c.tag == Justified
}

func (c *SegmentID) Genesis() bool {
	return c.tag == Genesis
}

func (c *SegmentID) GetSlot() *uint64 {
	return c.slot
}

func (c *SegmentID) GetRoot() *common.Hash {
	return c.root
}

func EpochFromRequest(r *http.Request) (uint64, error) {
	// Must only be a number
	regex := regexp.MustCompile(`^\d+$`)
	epoch := chi.URLParam(r, "epoch")
	if !regex.MatchString(epoch) {
		return 0, errors.New("invalid path variable: {epoch}")
	}
	epochMaybe, err := strconv.ParseUint(epoch, 10, 64)
	if err != nil {
		return 0, err
	}
	return epochMaybe, nil
}

func StringFromRequest(r *http.Request, name string) (string, error) {
	str := chi.URLParam(r, name)
	if str == "" {
		return "", nil
	}
	return str, nil
}

func BlockIdFromRequest(r *http.Request) (*SegmentID, error) {
	regex := regexp.MustCompile(`^(?:0x[0-9a-fA-F]{64}|head|finalized|genesis|\d+)$`)

	blockId := chi.URLParam(r, "block_id")
	if !regex.MatchString(blockId) {
		return nil, errors.New("invalid path variable: {block_id}")
	}

	if blockId == "head" {
		return &SegmentID{tag: Head}, nil
	}
	if blockId == "finalized" {
		return &SegmentID{tag: Finalized}, nil
	}
	if blockId == "genesis" {
		return &SegmentID{tag: Genesis}, nil
	}
	slotMaybe, err := strconv.ParseUint(blockId, 10, 64)
	if err == nil {
		return &SegmentID{slot: &slotMaybe}, nil
	}
	root := common.HexToHash(blockId)
	return &SegmentID{
		root: &root,
	}, nil
}

func StateIdFromRequest(r *http.Request) (*SegmentID, error) {
	regex := regexp.MustCompile(`^(?:0x[0-9a-fA-F]{64}|head|finalized|genesis|justified|\d+)$`)

	stateId := chi.URLParam(r, "state_id")
	if !regex.MatchString(stateId) {
		return nil, errors.New("invalid path variable: {state_id}")
	}

	if stateId == "head" {
		return &SegmentID{tag: Head}, nil
	}
	if stateId == "finalized" {
		return &SegmentID{tag: Finalized}, nil
	}
	if stateId == "genesis" {
		return &SegmentID{tag: Genesis}, nil
	}
	if stateId == "justified" {
		return &SegmentID{tag: Justified}, nil
	}
	slotMaybe, err := strconv.ParseUint(stateId, 10, 64)
	if err == nil {
		return &SegmentID{slot: &slotMaybe}, nil
	}
	root := common.HexToHash(stateId)
	return &SegmentID{
		root: &root,
	}, nil
}

func HashFromQueryParams(r *http.Request, name string) (*common.Hash, error) {
	hashStr := r.URL.Query().Get(name)
	if hashStr == "" {
		return nil, nil
	}
	// check if hashstr is an hex string
	if len(hashStr) != 2+2*32 {
		return nil, errors.New("invalid hash length")
	}
	if hashStr[:2] != "0x" {
		return nil, errors.New("invalid hash prefix")
	}
	notHex, err := regexp.MatchString("[^0-9A-Fa-f]", hashStr[2:])
	if err != nil {
		return nil, err
	}
	if notHex {
		return nil, errors.New("invalid hash characters")
	}

	hash := common.HexToHash(hashStr)
	return &hash, nil
}

// uint64FromQueryParams retrieves a number from the query params, in base 10.
func Uint64FromQueryParams(r *http.Request, name string) (*uint64, error) {
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

// decode a list of strings from the query params
func StringListFromQueryParams(r *http.Request, name string) ([]string, error) {
	values := r.URL.Query()[name]
	if len(values) == 0 {
		return nil, nil
	}

	// Combine all values into a single string, separating by comma
	str := strings.Join(values, ",")

	return regexp.MustCompile(`\s*,\s*`).Split(str, -1), nil
}
