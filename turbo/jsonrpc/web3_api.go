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

package jsonrpc

import (
	"context"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/crypto"

	"github.com/erigontech/erigon/v3/turbo/rpchelper"
)

// Web3API provides interfaces for the web3_ RPC commands
type Web3API interface {
	ClientVersion(_ context.Context) (string, error)
	Sha3(_ context.Context, input hexutility.Bytes) hexutility.Bytes
}

type Web3APIImpl struct {
	*BaseAPI
	ethBackend rpchelper.ApiBackend
}

// NewWeb3APIImpl returns Web3APIImpl instance
func NewWeb3APIImpl(ethBackend rpchelper.ApiBackend) *Web3APIImpl {
	return &Web3APIImpl{
		BaseAPI:    &BaseAPI{},
		ethBackend: ethBackend,
	}
}

// ClientVersion implements web3_clientVersion. Returns the current client version.
func (api *Web3APIImpl) ClientVersion(ctx context.Context) (string, error) {
	return api.ethBackend.ClientVersion(ctx)
}

// Sha3 implements web3_sha3. Returns Keccak-256 (not the standardized SHA3-256) of the given data.
func (api *Web3APIImpl) Sha3(_ context.Context, input hexutility.Bytes) hexutility.Bytes {
	return crypto.Keccak256(input)
}
