package contracts

//go:generate solc --base-path . --include-path ./../../build --abi --bin --overwrite --optimize -o build Sequencer.sol KeyperSet.sol KeyperSetManager.sol KeyBroadcastContract.sol ValidatorRegistry.sol
//go:generate abigen -abi build/Sequencer.abi -bin build/Sequencer.bin -pkg contracts -type sequencer -out ./gen_sequencer.go
//go:generate abigen -abi build/KeyperSet.abi -bin build/KeyperSet.bin -pkg contracts -type keyperSet -out ./gen_keyper_set.go
//go:generate abigen -abi build/KeyperSetManager.abi -bin build/KeyperSetManager.bin -pkg contracts -type keyperSetManager -out ./gen_keyper_set_manager.go
//go:generate abigen -abi build/KeyBroadcastContract.abi -bin build/KeyBroadcastContract.bin -pkg contracts -type keyBroadcastContract -out ./gen_key_broadcast_contract.go
//go:generate abigen -abi build/ValidatorRegistry.abi -bin build/ValidatorRegistry.bin -pkg contracts -type validatorRegistry -out ./gen_validator_registry.go
