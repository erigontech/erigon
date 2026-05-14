# SSZ-REST Engine API Kurtosis Devnet

This devnet runs local Erigon as EL, Prysm as CL, and Spamoor transaction load with a 4 second slot time. Erigon's authenticated Engine API port is used for both JSON-RPC bootstrap negotiation and SSZ-REST Engine API calls.

## Prerequisites

- Docker
- Kurtosis CLI
- A Prysm image built from OffchainLabs/prysm PR `16447`, or an equivalent image that advertises and uses SSZ-REST Engine API endpoint capabilities.

Use Prysm images built from the PR branch and tag them for this devnet:

```bash
docker tag local-prysm:ssz-rest-16447 local-prysm:ssz-rest
docker tag local-prysm-validator:ssz-rest-16447 local-prysm-validator:ssz-rest
```

## Run

From the Erigon repo root:

```bash
./devnet/ssz-rest-kurtosis/run.sh
```

The script builds `local-erigon:ssz-rest`, launches a Kurtosis Ethereum devnet, starts Spamoor, and prints the enclave name plus log commands for Erigon, Prysm, and Spamoor.

By default it pins `github.com/ethpandaops/ethereum-package@87df9d9d15e493ea91db788161e54324c39f6a6e`, the pre-Heze package revision compatible with the Prysm PR `16447` image. Override `ETHEREUM_PACKAGE` only when using a Prysm build that supports newer package config fields.

The generated Kurtosis params explicitly set:

- `seconds_per_slot: 4`
- EL client: local Erigon image
- CL client: Prysm image with PR `16447` SSZ-REST semantics
- Engine API JWT auth enabled on the normal authenticated engine port
- Spamoor load pointed at the devnet public RPC endpoint
- Deneb/Electra active at genesis; later fork epochs stay on the pinned package defaults so the generated Prysm execution genesis header matches Erigon's imported genesis block hash.

## Evidence Checks

After the network has run for at least 10 minutes:

```bash
kurtosis enclave inspect ssz-rest-erigon
kurtosis service logs ssz-rest-erigon el-1-erigon-prysm
kurtosis service logs ssz-rest-erigon cl-1-prysm-erigon
kurtosis service logs ssz-rest-erigon spamoor
```

Look for:

- Prysm exchanging capabilities with Erigon and receiving strings such as `POST /engine/v4/payloads`.
- Erigon log lines containing `[SSZ-REST] handled forkchoice`, `[SSZ-REST] handled new payload`, `[SSZ-REST] handled get payload`, or `[SSZ-REST] handled get blobs`.
- Produced and finalized beacon blocks in Prysm logs.
- Spamoor successful transaction submission and inclusion without Engine API auth failures, malformed SSZ loops, invalid payload loops, panics, or repeated fallback-to-JSON after SSZ capabilities are negotiated.

## Cleanup

```bash
kurtosis enclave rm -f ssz-rest-erigon
```
