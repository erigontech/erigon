package contracts

import (
	_ "embed"
)

//go:embed block_reward.json
var BlockReward []byte

//go:embed registrar.json
var Registrar []byte
