---
description: 'Erigon GraphQL Interface: EIP-1767 Ethereum Node Data API'
---

# GraphQL

Erigon implements the standard Ethereum GraphQL interface defined in [EIP-1767](https://eips.ethereum.org/EIPS/eip-1767), following the same schema as Geth.

## Enabling GraphQL

GraphQL is disabled by default. Enable it with the `--graphql` flag:

```bash
./erigon --graphql
```

GraphQL shares the same port as the HTTP JSON-RPC server (default `8545`). It does **not** use `--http.api` — `--graphql` is its own toggle.

## Endpoints

| Endpoint | Purpose |
|----------|---------|
| `http://localhost:8545/graphql` | GraphQL API endpoint |
| `http://localhost:8545/graphql/ui` | GraphiQL browser UI for interactive queries |

## Schema Reference

Erigon follows the Geth GraphQL schema. For the full schema reference, query examples, and field descriptions, see:

{% embed url="https://geth.ethereum.org/docs/interacting-with-geth/rpc/graphql" %}

The schema includes queries for blocks, transactions, accounts, logs, and pending state, as well as the `sendRawTransaction` mutation.

## Example Query

```graphql
{
  block(number: 21000000) {
    hash
    number
    timestamp
    transactions {
      hash
      from { address }
      to { address }
      value
    }
  }
}
```

```bash
curl -X POST http://localhost:8545/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ block(number: 21000000) { hash number } }"}'
```
