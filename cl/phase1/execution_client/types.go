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

package execution_client

import "github.com/erigontech/erigon/execution/engineapi/engine_types"

type PayloadStatus int

const (
	PayloadStatusNone = iota
	PayloadStatusNotValidated
	PayloadStatusInvalidated
	PayloadStatusValidated
)

func newPayloadStatusByEngineStatus(status engine_types.EngineStatus) PayloadStatus {
	switch status {
	case engine_types.SyncingStatus, engine_types.AcceptedStatus:
		return PayloadStatusNotValidated
	case engine_types.InvalidStatus, engine_types.InvalidBlockHashStatus:
		return PayloadStatusInvalidated
	case engine_types.ValidStatus:
		return PayloadStatusValidated
	}
	return PayloadStatusNone
}
