package otscontracts

import (
	_ "embed"
)

//go:embed erc20.json
var ERC20 []byte

//go:embed erc165.json
var ERC165 []byte

//go:embed IERC4626.json
var IERC4626 []byte

//go:embed junk.json
var Junk []byte
