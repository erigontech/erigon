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

package sentry_candidates

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon/eth/protocols/eth"
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
