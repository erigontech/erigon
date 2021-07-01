package auraabi

//go:generate abigen -abi ./../contracts/block_reward.json -pkg auraabi -type block_reward -out ./gen_block_reward.go
//go:generate abigen -abi ./../contracts/validator_set.json -pkg auraabi -type validator_set -out ./gen_validator_set.go
