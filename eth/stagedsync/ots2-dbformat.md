# Otterscan V2 DB Format

## Match tables

Match tables are dupsorted tables and have the following format:

|     |                     |              |
|-----|---------------------|--------------|
| `k` | `uint64`            | block number |
| `v` | `[length.Addr]byte` | address      |
|     | `uint64`            | incarnation (if > 0) |

This format enables some properties:

### Determine how many matches are in a given block

Given block number N, `var blockNum uint64 = <N>`, `var key [length.BlockNum]byte = <blockNum-as-bytes>` do: `k, v, err := cursor.SeekExact(key)`

If `k` is `nil`, it means the block was not found, hence no matches in that block.

If `k` is not `nil`, `cursor.CountDuplicates()` will give you the number of matches.

## Counter tables

Counter tables are non-dupsorted tables and have the following format:

|     |          |                    |
|-----|----------|--------------------|
| `k` | `uint64` | cumulative counter |
| `v` | `uint64` | block number       |

Which reads as: for each record, there are `k` matches up to (inclusive) block number `v`.

This format enables some properties:

TODO: describe what can be done with this format regarding pagination.

## Address attributes table

This non-dupsorted table contains a plain state of `address` -> bitmap of attributes.

Attributes are arbitrary tags defined by classifier implementations. E.g.: an address can be marked as an ERC20 token, or ERC721 NFT.

## Transfer Index tables

They contain indexes of address appearances under a certain context. One example of such context is the address being a from/to of an ERC20 transfer.

Transfer index tables are non-dupsorted and have the following format:

|     |                     |         |
|-----|---------------------|---------|
| `k` | `[length.Addr]byte` | address |
|     | `uint64`            | chunk ID: 0xffffffffffffffff means last chunk |
| `v` | `uint64` * N        | block numbers; divide them by 8 bytes to determine how many blocks are present in the index |

## Transfer Count tables

They are dupsorted table and contain pagination support metadata for the transfer index tables.

|     |                     |         |
|-----|---------------------|---------|
| `k` | `[length.Addr]byte` | address |
| `v` | `uint64`            | cumulative count of matches so far |
|     | `uint64`            | chunk ID: 0xffffffffffffffff means last chunk |

### Special optimization case 1

If the total count for a certain address is <= 256 and it fits in just 1 chunk, save it as:

|     |                     |         |
|-----|---------------------|---------|
| `k` | `[length.Addr]byte` | address |
| `v` | `uint8`             | (total count of matches) - 1 |

Consumer of that table must certify that's the case for a certain address by checking:

1. `cursor.CountDuplicates() == 1`
2. `len(v) == 1`

The goal of this optimization is to optimize for the most common case which is: "address is used for testing or is involved in just a few transactions" by storing just 1 byte instead of 16 in `v`.

## Holder Index tables

They contain records of first token appearances for a certain address.

Transfer index tables are dupsorted and have the following format:

|     |                     |                |
|-----|---------------------|----------------|
| `k` | `[length.Addr]byte` | holder address |
| `v` | `[length.Addr]byte` | token address  |
|     | `uint64`            | ethTx of first appearance (i.e. first tx from genesis that allowed us to identify that address as a holder of that token) |
