package sentry_candidates

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/nxadm/tail"
)

type Log struct {
	reader LineReader
}

type LogEvent struct {
	Timestamp    time.Time `json:"t"`
	Message      string    `json:"msg"`
	PeerIDHex    string    `json:"peer,omitempty"`
	NodeURL      string    `json:"nodeURL,omitempty"`
	ClientID     string    `json:"clientID,omitempty"`
	Capabilities []string  `json:"capabilities,omitempty"`
}

func NewLog(reader LineReader) *Log {
	return &Log{reader}
}

func (log *Log) Read() (*LogEvent, error) {
	var event LogEvent
	for event.Message != "Sentry peer did Connect" {
		line, err := log.reader.ReadLine()
		if (err != nil) || (line == nil) {
			return nil, err
		}

		lineData := []byte(*line)
		if err := json.Unmarshal(lineData, &event); err != nil {
			return nil, err
		}
	}
	return &event, nil
}

func (event *LogEvent) EthVersion() uint {
	var maxVersion uint64
	for _, capability := range event.Capabilities {
		if !strings.HasPrefix(capability, eth.ProtocolName) {
			continue
		}
		versionStr := capability[len(eth.ProtocolName)+1:]
		version, _ := strconv.ParseUint(versionStr, 10, 32)
		if version > maxVersion {
			maxVersion = version
		}
	}
	return uint(maxVersion)
}

type LineReader interface {
	ReadLine() (*string, error)
}

type ScannerLineReader struct {
	scanner *bufio.Scanner
}

func NewScannerLineReader(reader io.Reader) *ScannerLineReader {
	return &ScannerLineReader{bufio.NewScanner(reader)}
}

func (reader *ScannerLineReader) ReadLine() (*string, error) {
	if reader.scanner.Scan() {
		line := reader.scanner.Text()
		return &line, nil
	} else {
		return nil, reader.scanner.Err()
	}
}

type TailLineReader struct {
	ctx  context.Context
	tail *tail.Tail
}

func NewTailLineReader(ctx context.Context, tail *tail.Tail) *TailLineReader {
	return &TailLineReader{ctx, tail}
}

func (reader *TailLineReader) ReadLine() (*string, error) {
	select {
	case line, ok := <-reader.tail.Lines:
		if ok {
			return &line.Text, nil
		} else {
			return nil, reader.tail.Err()
		}
	case <-reader.ctx.Done():
		return nil, reader.ctx.Err()
	}
}
