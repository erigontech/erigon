package cli

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/ledgerwatch/turbo-geth/rpc"
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

	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var allowListFile allowListFile

	err = json.Unmarshal(fileContents, &allowListFile)
	if err != nil {
		return nil, err
	}

	return allowListFile.Allow, nil
}
