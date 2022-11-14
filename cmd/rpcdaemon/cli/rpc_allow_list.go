package cli

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/ledgerwatch/erigon/rpc"
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
