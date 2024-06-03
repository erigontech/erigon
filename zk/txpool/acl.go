package txpool

import (
	"github.com/ledgerwatch/erigon-lib/kv"
)

const (
	Config    = "Config"
	Whitelist = "Whitelist"
	Blacklist = "Blacklist"
)

var ACLTables = []string{
	Config,
	Whitelist,
	Blacklist,
}

var ACLTablesCfg = kv.TableCfg{}

const ACLDB kv.Label = 255

func init() {
	for _, name := range ACLTables {
		_, ok := ACLTablesCfg[name]
		if !ok {
			ACLTablesCfg[name] = kv.TableCfgItem{}
		}
	}
}
