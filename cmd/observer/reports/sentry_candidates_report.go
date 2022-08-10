package reports

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/cmd/observer/observer/sentry_candidates"
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
