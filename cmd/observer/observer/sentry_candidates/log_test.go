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
	"context"
	"strings"
	"testing"

	"github.com/nxadm/tail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogRead(t *testing.T) {
	line := `
{"capabilities":["eth/66","wit/0"],"clientID":"Nethermind/v1.13.0-0-2e8910b5b-20220520/X64-Linux/6.0.4","lvl":"dbug","msg":"Sentry peer did Connect","nodeURL":"enode://4293b17b897abed4a88d6e760e86a4bb700d62c12a9411fbf9ec0c9df3740c8670b184bd9f24d163cbd9bf05264b3047a69f079209d53d2e0dc05dd678d07cf0@1.2.3.4:45492","peer":"93b17b897abed4a88d6e760e86a4bb700d62c12a9411fbf9ec0c9df3740c8670b184bd9f24d163cbd9bf05264b3047a69f079209d53d2e0dc05dd678d07cf000","t":"2022-05-31T11:10:19.032092272Z"}
`
	line = strings.TrimLeft(line, "\r\n ")
	eventLog := NewLog(NewScannerLineReader(strings.NewReader(line)))
	event, err := eventLog.Read()
	assert.Nil(t, err)
	require.NotNil(t, event)

	assert.NotEmpty(t, event.Message)
	assert.NotEmpty(t, event.PeerIDHex)
	assert.NotEmpty(t, event.NodeURL)
	assert.NotEmpty(t, event.ClientID)

	assert.Equal(t, int64(1653995419), event.Timestamp.Unix())
	assert.Equal(t, "Sentry peer did Connect", event.Message)
	assert.True(t, strings.HasPrefix(event.NodeURL, "enode:"))
	assert.True(t, strings.HasPrefix(event.ClientID, "Nethermind"))
	assert.Equal(t, 2, len(event.Capabilities))
}

func TestLogReadTailSkimFile(t *testing.T) {
	t.Skip()

	logFile, err := tail.TailFile(
		"erigon.log",
		tail.Config{Follow: false, MustExist: true})
	require.Nil(t, err)
	defer func() {
		_ = logFile.Stop()
	}()

	eventLog := NewLog(NewTailLineReader(context.Background(), logFile))
	for {
		event, err := eventLog.Read()
		require.Nil(t, err)
		if event == nil {
			break
		}
	}
}

func TestLogEventEthVersion(t *testing.T) {
	event := LogEvent{}
	event.Capabilities = []string{"wit/0", "eth/64", "eth/65", "eth/66"}
	version := event.EthVersion()
	assert.Equal(t, uint(66), version)
}
