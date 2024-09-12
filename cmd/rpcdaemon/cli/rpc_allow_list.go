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

package cli

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/erigontech/erigon/rpc"
)

type allowListFile struct {
	Allow rpc.AllowList `json:"allow"`
}

func parseAllowListForRPC(path string) (rpc.AllowList, error) {
	path = strings.TrimSpace(path)
	if path == "" { // no file is provided
		return nil, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		file.Close() //nolint: errcheck
	}()

	fileContents, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var allowListFileObj allowListFile

	err = json.Unmarshal(fileContents, &allowListFileObj)
	if err != nil {
		return nil, err
	}

	return allowListFileObj.Allow, nil
}
