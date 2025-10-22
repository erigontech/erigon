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

package heimdall

type Status struct {
	LatestBlockHash string `json:"latest_block_hash"`
	LatestAppHash   string `json:"latest_app_hash"`
	LatestBlockTime string `json:"latest_block_time"`
	CatchingUp      bool   `json:"catching_up"`
}

type StatusResponse struct {
	Height string `json:"height"`
	Result Status `json:"result"`
}
