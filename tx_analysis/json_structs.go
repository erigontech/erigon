package main

import (
	"github.com/ledgerwatch/erigon/core/types"
)

type params_h struct {
	Subscription string       `json:"subscription"`
	Result       types.Header `json:"result"`
}

type json_resp_header struct {
	JsonRPC float32  `json:"jsonrpc,string"`
	Method  string   `json:"method"`
	Params  params_h `json:"params"`
	Result  string   `json:"result"`
}
