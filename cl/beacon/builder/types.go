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

package builder

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const maxUint256DecimalDigits = 78

var ErrInvalidBidSignature = errors.New("invalid builder bid signature")

type ExecutionHeader struct {
	Version string              `json:"version"`
	Data    ExecutionHeaderData `json:"data"`
}

type ExecutionHeaderData struct {
	Message   ExecutionHeaderMessage `json:"message"`
	Signature common.Bytes96         `json:"signature"`
}

type ExecutionHeaderMessage struct {
	Header             *cltypes.Eth1Header                    `json:"header"`
	BlobKzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment] `json:"blob_kzg_commitments"`
	ExecutionRequests  *cltypes.ExecutionRequests             `json:"execution_requests,omitempty"`
	Value              string                                 `json:"value"`
	PubKey             common.Bytes48                         `json:"pubkey"`
}

func (h ExecutionHeader) BlockValue() *big.Int {
	rawValue := h.Data.Message.Value
	if rawValue == "" {
		return nil
	}
	value := strings.TrimLeft(rawValue, "0")
	if value == "" {
		return new(big.Int)
	}
	if len(value) > maxUint256DecimalDigits {
		log.Warn("builder block value outside uint256 range", "length", len(rawValue))
		return nil
	}
	if value[0] < '0' || value[0] > '9' {
		log.Warn("invalid builder block value syntax", "length", len(rawValue))
		return nil
	}
	blockValue, ok := new(big.Int).SetString(value, 10)
	if !ok {
		log.Warn("invalid builder block value syntax", "length", len(rawValue))
		return nil
	}
	if blockValue.BitLen() > 256 {
		log.Warn("builder block value outside uint256 range", "length", len(rawValue))
		return nil
	}
	return blockValue
}

// SigningRoot returns the root the builder signs for a bid of the given fork. The header must already carry that
// fork's version, because it selects the fields of the header root.
func (h *ExecutionHeader) SigningRoot(cfg *clparams.BeaconChainConfig, version clparams.StateVersion) ([32]byte, error) {
	message := &h.Data.Message
	value := h.BlockValue()
	switch {
	case version >= clparams.GloasVersion:
		return [32]byte{}, fmt.Errorf("no builder API bid for fork %s", version)
	case value == nil:
		return [32]byte{}, fmt.Errorf("invalid builder block value %q", message.Value)
	case message.Header == nil:
		return [32]byte{}, errors.New("missing execution payload header")
	case version >= clparams.DenebVersion && message.BlobKzgCommitments == nil:
		return [32]byte{}, errors.New("missing blob KZG commitments")
	case version >= clparams.ElectraVersion && message.ExecutionRequests == nil:
		return [32]byte{}, errors.New("missing execution requests")
	}
	// Builder API messages are signed with the genesis fork version and an empty genesis validators root, so the
	// domain does not change across forks.
	domain, err := fork.ComputeDomain(cfg.DomainApplicationBuilder[:], utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)), [32]byte{})
	if err != nil {
		return [32]byte{}, err
	}
	return fork.ComputeSigningRoot(builderBid{message: message, value: value, version: version}, domain)
}

// VerifySignature checks the bid signature against the pubkey in the bid.
func (h *ExecutionHeader) VerifySignature(cfg *clparams.BeaconChainConfig, version clparams.StateVersion) error {
	root, err := h.SigningRoot(cfg, version)
	if err != nil {
		return err
	}
	valid, err := bls.Verify(h.Data.Signature[:], root[:], h.Data.Message.PubKey[:])
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidBidSignature, err)
	}
	if !valid {
		return ErrInvalidBidSignature
	}
	return nil
}

// builderBid is the builder-specs BuilderBid container of a pre-Gloas fork.
type builderBid struct {
	message *ExecutionHeaderMessage
	value   *big.Int
	version clparams.StateVersion
}

func (b builderBid) HashSSZ() ([32]byte, error) {
	var value [32]byte
	b.value.FillBytes(value[:])
	slices.Reverse(value[:])
	schema := []any{b.message.Header}
	if b.version >= clparams.DenebVersion {
		schema = append(schema, b.message.BlobKzgCommitments)
	}
	if b.version >= clparams.ElectraVersion {
		schema = append(schema, b.message.ExecutionRequests)
	}
	schema = append(schema, value[:], b.message.PubKey[:])
	return merkle_tree.HashTreeRoot(schema...)
}

type BlindedBlockResponse struct {
	Version string          `json:"version"`
	Data    json.RawMessage `json:"data"`
}
