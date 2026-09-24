# lightclient

Vendored, verbatim, from bsc-erigon (`core/vm/lightclient/{v1,v2,iavl}`) — the
Tendermint/CometBFT light-client and IAVL merkle-proof code backing BSC's
`0x64`/`0x65`/`0x67` cross-chain precompiles. Kept byte-identical (only the
internal import path is rewritten) so it stays consensus-exact with upstream;
excluded from lint/gofmt.
