# Background

A tool for generating a test case for a given tracer. It communicates to a node via rpc to build:

1. `genesis` using the parent block of the given `txnHash` and its pre-state
2. `config` for the chain by using the node `admin_nodeInfo` API
3. `input` which is the raw transaction bytes of the given `txnHash`
4. `result` which is result of tracing the given `txnHash` with the given `tracerConfig`

# Pre-requisites

1. install node.js & npm: https://docs.npmjs.com/downloading-and-installing-node-js-and-npm

# Usage

```
cd eth/tracers/internal/tracetest/testgenerator
npm install
npm start -- \
--rpcUrl=http://localhost:8545 \
--txnHash=0x5c08231a3c34bd1be6ac7df553d451246175f3dad3245ab5e4413cde35ace52e \
--traceConfig='{"tracer": "prestateTracer", "tracerConfig": { "diffMode": true } }' \
--outputFilePath=../testdata/prestate_tracer_with_diff_mode/create_with_value.json
```
