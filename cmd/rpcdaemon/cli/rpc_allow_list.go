package cli

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/ledgerwatch/turbo-geth/rpc"
)

type allowListFile struct {
	Allow rpc.AllowList `json:"allow"`
}

func parseAllowListForRPC(path string) rpc.AllowList {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	var allowListFile allowListFile

	err = json.Unmarshal(fileContents, &allowListFile)
	if err != nil {
		log.Fatal(err)
	}

	return allowListFile.Allow
}
