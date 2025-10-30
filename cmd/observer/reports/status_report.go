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

package reports

import (
	"context"
	"fmt"
	"strings"

	"github.com/erigontech/erigon/cmd/observer/database"
)

type StatusReport struct {
	TotalCount      uint
	DistinctIPCount uint
}

func CreateStatusReport(ctx context.Context, db database.DB, maxPingTries uint, networkID uint) (*StatusReport, error) {
	totalCount, err := db.CountNodes(ctx, maxPingTries, networkID)
	if err != nil {
		return nil, err
	}

	distinctIPCount, err := db.CountIPs(ctx, maxPingTries, networkID)
	if err != nil {
		return nil, err
	}

	report := StatusReport{
		totalCount,
		distinctIPCount,
	}
	return &report, nil
}

func (report *StatusReport) String() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("total: %d", report.TotalCount))
	builder.WriteRune('\n')
	builder.WriteString(fmt.Sprintf("distinct IPs: %d", report.DistinctIPCount))
	builder.WriteRune('\n')
	return builder.String()
}
