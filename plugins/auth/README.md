# plugins/auth — UCAN Authorization

UCAN-based capability authorization for the Erigon plugin ecosystem. Provides delegatable, attenuated authorization with on-chain revocation.

**Design:** https://github.com/erigontech/cocoon/tree/master/pocs/auth

## What's Implemented

- **DID parsing** — `did:pkh:eip155:{chainId}:{address}` and `did:key:{multibase}`
- **Capability model** — hierarchical commands (`/storage/*`), wildcard matching, attenuation validation
- **UCAN Token** — structure validation, time bounds, CID computation, payload serialization
- **Delegation chain verifier** — recursive proof resolution, depth-limited, attenuation-checked
- **EOA Signer** — ERC-191 personal_sign signature creation and verification (real ECDSA)
- **Store interface** — `MemoryStore` (testing) and `MDBXStore` (production, MDBX-backed)
- **21 tests** — DID, capabilities, tokens, EOA signatures, full delegation chains with real crypto, MDBX store

## What's Next

- [ ] ERC-1271 contract wallet signature verification (needs `eth_call`)
- [ ] On-chain revocation registry integration (needs `eth_call`)
- [ ] RPC middleware — validate UCAN on authenticated RPC calls
- [ ] Component provider — wrap as `component.Component[AuthProvider]`
- [ ] Plugin registration — `init()` + `components.Register("auth", ...)`
- [ ] EIP-8141 frame transaction validation (future, post-Hegota)
- [ ] Torrent-based token distribution via CCIP plugin
- [ ] DAG-CBOR encoding (currently JSON — switch to CBOR for spec compliance)
- [ ] did:key support for ML-KEM-768 public keys

## Configuration (future)

```toml
[auth]
enabled = true
revocation-registry = "0x..."  # on-chain revocation contract address
```

## Dependencies

- `common/crypto` — ECDSA signing, keccak256
- `db/kv/mdbx` — persistent token and revocation storage
- No dependency on component framework (yet) — pure library code
