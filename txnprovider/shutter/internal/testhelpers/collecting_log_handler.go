// Copyright 2025 The Erigon Authors
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

package testhelpers

import (
	"strings"

	"github.com/erigontech/erigon-lib/log/v3"
)

type CollectingLogHandler struct {
	records []*log.Record
	handler log.Handler
}

func NewCollectingLogHandler(handler log.Handler) *CollectingLogHandler {
	return &CollectingLogHandler{
		handler: handler,
	}
}

func (clh *CollectingLogHandler) Log(r *log.Record) error {
	clh.records = append(clh.records, r)
	return clh.handler.Log(r)
}

func (clh *CollectingLogHandler) ContainsAll(subStrs []string) bool {
	for _, subStr := range subStrs {
		if !clh.Contains(subStr) {
			return false
		}
	}
	return true
}

func (clh *CollectingLogHandler) Contains(subStr string) bool {
	for _, r := range clh.records {
		msg := string(log.TerminalFormatNoColor().Format(r))
		if strings.Contains(msg, subStr) {
			return true
		}
	}
	return false
}

func (clh *CollectingLogHandler) FormattedRecords() []string {
	formattedRecords := make([]string, len(clh.records))
	for i, record := range clh.records {
		formattedRecords[i] = strings.TrimSuffix(string(log.TerminalFormatNoColor().Format(record)), "\n")
	}
	return formattedRecords
}
