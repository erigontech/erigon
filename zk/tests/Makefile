# Description: Makefile for running tests

DEFAULT_ERIGON_URL = http://34.175.214.161:8500
DEFAULT_ZKEVM_URL = http://34.175.214.161:8505
DEFAULT_SEQUENCER_URL = http://34.175.214.161:8005
DEFAULT_BLOCKS = 1000
DEFAULT_DIFF = 10

## make compare-blocks - compare blocks from erigon, zkevm and sequencer
compare-blocks:
	go run ../debug_tools/nightly-block-compare/main.go -erigon $${ERIGON_URL:-$(DEFAULT_ERIGON_URL)} -zkevm $${ZKEVM_URL:-$(DEFAULT_ZKEVM_URL)} -sequencer $${SEQUENCER_URL:-$(DEFAULT_SEQUENCER_URL)} -blocks $${BLOCKS:-$(DEFAULT_BLOCKS)} -diff $${DIFF:-$(DEFAULT_DIFF)}

.PHONY: compare-blocks

## make run-bridge - run bridge tests
run-bridge:
	cd bridge && docker-compose up --build --exit-code-from bridge-script

.PHONY: run-bridge

## make help - print commands help.
help	:	Makefile
	@sed -n 's/^##//p' $<