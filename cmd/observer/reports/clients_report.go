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

	"github.com/erigontech/erigon/v3/cmd/observer/database"
	"github.com/erigontech/erigon/v3/cmd/observer/observer"
)

type ClientsReportEntry struct {
	Name  string
	Count uint
}

type ClientsReport struct {
	Clients []ClientsReportEntry
}

func CreateClientsReport(ctx context.Context, db database.DB, limit uint, maxPingTries uint, networkID uint) (*ClientsReport, error) {
	groups := make(map[string]uint)
	unknownCount := uint(0)
	enumFunc := func(clientID *string) {
		if clientID != nil {
			if observer.IsClientIDBlacklisted(*clientID) {
				return
			}
			clientName := observer.NameFromClientID(*clientID)
			groups[clientName]++
		} else {
			unknownCount++
		}
	}
	if err := db.EnumerateClientIDs(ctx, maxPingTries, networkID, enumFunc); err != nil {
		return nil, err
	}

	totalCount := sumMapValues(groups)

	report := ClientsReport{}

	for i := uint(0); i < limit; i++ {
		clientName, count := takeMapMaxValue(groups)
		if count == 0 {
			break
		}

		client := ClientsReportEntry{
			clientName,
			count,
		}
		report.Clients = append(report.Clients, client)
	}

	othersCount := sumMapValues(groups)

	report.Clients = append(report.Clients,
		ClientsReportEntry{"...", othersCount},
		ClientsReportEntry{"total", totalCount},
		ClientsReportEntry{"unknown", unknownCount})

	return &report, nil
}

func (report *ClientsReport) String() string {
	var builder strings.Builder
	builder.Grow(2 + 2*len(report.Clients))
	builder.WriteString("clients:")
	builder.WriteRune('\n')
	for _, client := range report.Clients {
		builder.WriteString(fmt.Sprintf("%6d %s", client.Count, client.Name))
		builder.WriteRune('\n')
	}
	return builder.String()
}

func takeMapMaxValue(m map[string]uint) (string, uint) {
	maxKey := ""
	maxValue := uint(0)

	for k, v := range m {
		if v > maxValue {
			maxKey = k
			maxValue = v
		}
	}

	delete(m, maxKey)
	return maxKey, maxValue
}

func sumMapValues(m map[string]uint) uint {
	sum := uint(0)
	for _, v := range m {
		sum += v
	}
	return sum
}
