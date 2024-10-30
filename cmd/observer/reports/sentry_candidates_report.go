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
	"net/url"
	"os"
	"strings"

	"github.com/erigontech/erigon/v3/cmd/observer/database"
	"github.com/erigontech/erigon/v3/cmd/observer/observer/sentry_candidates"
)

type SentryCandidatesReport struct {
	TotalCount       uint
	SeenCount        uint
	HandshakeCount   uint
	UnknownClientIDs []string
	UnseenClientIDs  []string
}

func CreateSentryCandidatesReport(
	ctx context.Context,
	db database.DB,
	logPath string,
) (*SentryCandidatesReport, error) {
	logFile, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = logFile.Close()
	}()
	log := sentry_candidates.NewLog(sentry_candidates.NewScannerLineReader(logFile))

	report := SentryCandidatesReport{}

	for {
		event, err := log.Read()
		if err != nil {
			return nil, err
		}
		if event == nil {
			break
		}

		if event.NodeURL == "" {
			continue
		}
		nodeURL, err := url.Parse(event.NodeURL)
		if err != nil {
			return nil, err
		}
		id := database.NodeID(nodeURL.User.Username())

		knownAddr, err := db.FindNodeAddr(ctx, id)
		if err != nil {
			return nil, err
		}

		knownClientID, err := db.FindClientID(ctx, id)
		if err != nil {
			return nil, err
		}

		isSeen := knownAddr != nil
		isKnownClientID := knownClientID != nil

		report.TotalCount++
		if isSeen {
			report.SeenCount++
		} else {
			report.UnseenClientIDs = append(report.UnseenClientIDs, event.ClientID)
		}
		if isKnownClientID {
			report.HandshakeCount++
		} else {
			report.UnknownClientIDs = append(report.UnknownClientIDs, event.ClientID)
		}
	}

	return &report, nil
}

func (report *SentryCandidatesReport) String() string {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("total: %d", report.TotalCount))
	builder.WriteRune('\n')
	builder.WriteString(fmt.Sprintf("seen: %d", report.SeenCount))
	builder.WriteRune('\n')
	builder.WriteString(fmt.Sprintf("handshakes: %d", report.HandshakeCount))
	builder.WriteRune('\n')

	builder.WriteRune('\n')
	builder.WriteString("unseen:")
	builder.WriteRune('\n')
	for _, clientID := range report.UnseenClientIDs {
		builder.WriteString(clientID)
		builder.WriteRune('\n')
	}

	builder.WriteRune('\n')
	builder.WriteString("unknown:")
	builder.WriteRune('\n')
	for _, clientID := range report.UnknownClientIDs {
		builder.WriteString(clientID)
		builder.WriteRune('\n')
	}

	return builder.String()
}
