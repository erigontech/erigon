package snapshothashes

import (
	_ "embed"
	"encoding/json"
)

//go:embed erigon-snapshots/mainnet.json
var mainnet []byte
var Mainnet = fromJson(mainnet)

//go:embed erigon-snapshots/goerli.json
var goerli []byte
var Goerli = fromJson(goerli)

type Preverified map[string]string

func fromJson(in []byte) (out Preverified) {
	if err := json.Unmarshal(in, &out); err != nil {
		panic(err)
	}
	return out
}
