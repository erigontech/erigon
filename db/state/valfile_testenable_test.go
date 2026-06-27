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

package state

import "github.com/erigontech/erigon/db/state/statecfg"

// Run the whole db/state suite with the external value-file path on, so it is
// exercised by default (production gates it via ValueFileThreshold).
func init() {
	for _, d := range []*statecfg.DomainCfg{
		&statecfg.Schema.AccountsDomain,
		&statecfg.Schema.StorageDomain,
		&statecfg.Schema.CommitmentDomain,
		&statecfg.Schema.ReceiptDomain,
	} {
		if !d.LargeValues {
			d.ValueFileThreshold = 1
		}
	}
}
