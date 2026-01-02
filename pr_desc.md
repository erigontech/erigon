## Summary

Introduces a new `common/maphash` package that provides efficient map and LRU cache implementations using Go's `hash/maphash` for `[]byte` keys.

### Why maphash?

Go's `hash/maphash` is based on AES-NI instructions (on supported hardware) which provides:
- **Uniform distribution**: AES-based hashing produces highly uniform hash values, making collisions statistically negligible
- **Fast**: Hardware-accelerated on modern CPUs, significantly faster than cryptographic hashes like SHA256 or Keccak
- **Non-cryptographic but sufficient**: For cache keys, we don't need cryptographic security - we need speed and uniform distribution
- **Attacker-resistant**: The random seed is generated at runtime, making it impractical for attackers to craft collision attacks

### Collision handling

We intentionally do not handle hash collisions. With a 64-bit hash space and AES-based uniform distribution, the probability of collision is approximately 1 in 2^32 after 2^32 insertions (birthday bound). For our use cases (validator public key caches with ~1M entries), the collision probability is negligible (~10^-13).

### New types

- `maphash.Map[V]` - Thread-safe map with `[]byte` keys
- `maphash.LRU[V]` - Thread-safe LRU cache with `[]byte` keys (wraps hashicorp/golang-lru)
- `maphash.Hash([]byte)` - Direct hash function access
- `maphash.SetSeed(uint64)` - Set deterministic seed for reproducible hashing

### Migration

Replaced custom cache implementations:
- `cl/phase1/core/state`: `publicKeyIndicies` map now uses `maphash.Map[uint64]`
- `cl/utils/bls`: Simplified BLS public key cache from ~120 lines of custom sharded cache to ~60 lines using `maphash.Map`

### Performance characteristics

The previous BLS cache used manual sharding (sum of bytes % 16384) with sorted slices and binary search. The new implementation:
- Eliminates manual sharding logic
- Eliminates sorted slice maintenance and binary search overhead
- Uses O(1) hash table lookups instead of O(log n) binary search
- Reduces code complexity significantly
