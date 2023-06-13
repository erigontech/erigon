package contracts

import (
	_ "embed"
)

//go:embed block_reward.json
var BlockReward []byte

//go:embed certifier.json
var Certifier []byte

//go:embed registrar.json
var Registrar []byte

//go:embed withdrawal.json
var Withdrawal []byte

//go:embed block_gas_limit.json
var BlockGasLimit []byte
