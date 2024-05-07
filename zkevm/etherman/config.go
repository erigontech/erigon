package etherman

import (
	"github.com/ledgerwatch/erigon-lib/common"
)

// Config represents the configuration of the etherman
type Config struct {
	URL         string `mapstructure:"URL"`
	L1ChainID   uint64 `mapstructure:"L1ChainID"`
	L2ChainID   uint64 `mapstructure:"L2ChainID"`
	L2ChainName string `mapstructure:"L2ChainName"`

	PoEAddr                   common.Address `mapstructure:"PoEAddr"`
	MaticAddr                 common.Address `mapstructure:"MaticAddr"`
	GlobalExitRootManagerAddr common.Address `mapstructure:"GlobalExitRootManagerAddr"`

	PrivateKeyPath     string `mapstructure:"PrivateKeyPath"`
	PrivateKeyPassword string `mapstructure:"PrivateKeyPassword"`
}
