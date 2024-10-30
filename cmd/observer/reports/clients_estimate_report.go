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
	"math"
	"strings"

	"github.com/erigontech/erigon/v3/cmd/observer/database"
)

type ClientsEstimateReportEntry struct {
	Name      string
	CountLow  uint
	CountHigh uint
}

type ClientsEstimateReport struct {
	Clients []ClientsEstimateReportEntry
}

func CreateClientsEstimateReport(
	ctx context.Context,
	db database.DB,
	limit uint,
	maxPingTries uint,
	networkID uint,
) (*ClientsEstimateReport, error) {
	clientsReport, err := CreateClientsReport(ctx, db, limit, maxPingTries, networkID)
	if err != nil {
		return nil, err
	}

	report := ClientsEstimateReport{}

	for i, topClient := range clientsReport.Clients {
		if uint(i) >= limit {
			break
		}
		clientName := topClient.Name

		sameNetworkCount, err := db.CountClients(ctx, clientName+"/", maxPingTries, networkID)
		if err != nil {
			return nil, err
		}
		if sameNetworkCount == 0 {
			continue
		}

		knownNetworkCount, err := db.CountClientsWithNetworkID(ctx, clientName+"/", maxPingTries)
		if err != nil {
			return nil, err
		}
		if knownNetworkCount == 0 {
			continue
		}

		// 1 - (1 - p)/2 percentile for 95% confidence
		const z = 1.96
		intervalLow, intervalHigh := waldInterval(knownNetworkCount, sameNetworkCount, z)

		transientErrCount, err := db.CountClientsWithHandshakeTransientError(ctx, clientName+"/", maxPingTries)
		if err != nil {
			return nil, err
		}

		countLow := sameNetworkCount + uint(math.Round(float64(transientErrCount)*intervalLow))
		countHigh := sameNetworkCount + uint(math.Round(float64(transientErrCount)*intervalHigh))

		client := ClientsEstimateReportEntry{
			clientName,
			countLow,
			countHigh,
		}
		report.Clients = append(report.Clients, client)
	}

	return &report, nil
}

// https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Normal_approximation_interval_or_Wald_interval
func waldInterval(n uint, ns uint, z float64) (float64, float64) {
	nf := n - ns
	p := float64(ns) / float64(n)
	interval := z * math.Sqrt(float64(ns*nf)) / (float64(n) * math.Sqrt(float64(n)))
	return p - interval, p + interval
}

func (report *ClientsEstimateReport) String() string {
	var builder strings.Builder
	builder.Grow(2 * len(report.Clients))
	for _, client := range report.Clients {
		builder.WriteString(fmt.Sprintf("%6d - %-6d %s", client.CountLow, client.CountHigh, client.Name))
		builder.WriteRune('\n')
	}
	return builder.String()
}
