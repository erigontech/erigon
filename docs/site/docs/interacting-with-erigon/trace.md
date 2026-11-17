---
title: "trace"
description: "trace_ namespace: OpenEthereum-compatible transaction and block traces."
sidebar_position: 7
---


# trace

The `trace` module is for getting a deeper insight into transaction processing. It includes two sets of calls: the transaction trace filtering API and the ad-hoc tracing API.

In order to use the Transaction-Trace Filtering API, Erigon must be fully synced with the argument `trace` in the `--http.api` flag.

```bash
./build/bin/erigon --http.api=eth,erigon,trace
```

Methods that replay history need the block and the state history of the block they trace. A node keeps them inside its retention window: 262,144 blocks with `--prune.mode=full` or `--prune.mode=blocks`, 100,000 blocks with `--prune.mode=minimal`, and all blocks with `--prune.mode=archive`. A request for a block outside the window returns an error that names the first available block (see [Errors](#errors)).

## The Ad-hoc Tracing API

The ad-hoc tracing API allows you to perform diagnostics on calls or transactions—whether they are historical ones from the chain or hypothetical ones not yet mined.

The diagnostics are requested by providing an array of trace types, which selects the outputs to return:

* `trace`: Provides a Transaction Trace, showing the sequence of all external calls, internal messages, and value transfers that occurred during execution.
* `vmTrace`: Provides a Virtual Machine Execution Trace, delivering a full step-by-step trace of the EVM's state throughout the transaction, including all opcodes and gas usage.
* `stateDiff`: Provides a State Difference, detailing all altered portions of the Ethereum state (e.g., storage, balances, nonces) resulting from the transaction's execution.

The array may contain zero or more of these values. An empty array `[]` is valid and returns only the `output` of the execution. An unknown value returns `-32602`. Duplicates are accepted.

The `trace` output of every `trace_*` method is a flat list of call frames in the OpenEthereum format, each with an explicit `subtraces` count and `traceAddress` path. The `trace_*` methods do not accept a custom tracer: for Geth-style tracers, including `flatCallTracer`, use the `debug_trace*` methods.

#### Providing a Transaction to Trace

There are three primary ways to specify the transaction to be traced:

1. Hypothetical Call: Providing the transaction information (like sender, recipient, and data) as if making a call using `eth_call` (see `trace_call`).
2. Raw Transaction: Providing raw, signed transaction data, as when using `eth_sendRawTransaction` (see `trace_rawTransaction`).
3. Mined Transaction: Providing a transaction hash for a previously mined transaction (see `trace_replayTransaction`).

:::info
**Note**: Replaying a mined transaction needs its block and state history, so the transaction must be inside the node's retention window (all blocks on an archive node).
:::

## The Transaction-Trace Filtering API

These APIs allow you to get a full _externality_ trace on any transaction executed throughout the Erigon chain. Unlike the log filtering API, you are able to search and filter based only upon address information. Information returned includes the execution of all `CREATE`s, `SUICIDE`s and all variants of `CALL` together with input data, output data, gas usage, amount transferred and the success status of each individual action.

### `traceAddress` field

The `traceAddress` field of all returned traces, gives the exact location in the call trace \[index in root, index in first `CALL`, index in second `CALL`, ...].

i.e. if the trace is:

```text
A
  CALLs B
    CALLs G
  CALLs C
    CALLs G
```

then it should look something like:

`[ {A: []}, {B: [0]}, {G: [0, 0]}, {C: [1]}, {G: [1, 0]} ]`

Frames that are omitted from the output (see [Precompile frames](#precompile-frames)) do not take an index, so the indexes of the emitted children are always consecutive.

## Optional parameters and trace settings

Besides the parameters described for each method, Erigon accepts optional trailing parameters. They are positional, so the parameters before them must be supplied even when they are not otherwise needed.

| Method | Optional trailing parameters |
| --- | --- |
| `trace_call` | 4. trace settings |
| `trace_callMany` | 3. trace settings |
| `trace_rawTransaction` | none (a third parameter returns `-32602`) |
| `trace_replayTransaction`, `trace_replayBlockTransactions` | 3. `gasBailOut`, 4. trace settings |
| `trace_block`, `trace_transaction` | 2. `gasBailOut`, 3. trace settings |
| `trace_get` | 3. `gasBailOut`, 4. trace settings |
| `trace_filter` | 2. `gasBailOut`, 3. trace settings |

`gasBailOut` is a boolean, `false` by default; see [The gasBailOut option](#the-gasbailout-option).

The trace settings object accepts the following members. Member names are matched case-insensitively, and unknown members are ignored.

| Member | Type | Accepted by | Description |
| --- | --- | --- | --- |
| `stateOverrides` | Object | `trace_call`, `trace_callMany` | State overrides in the `eth_call` format (`balance`, `nonce`, `code`, `state`, `stateDiff`, `movePrecompileToAddress`), applied before execution; in `trace_callMany` once, before the first call. The `stateDiff` output is relative to the overridden state. Setting both `state` and `stateDiff` for an account returns an error. |
| `blockOverrides` | Object | all methods with trace settings | Replaces fields of the block environment the transactions run with (for example `number`, `time`, `baseFeePerGas`, `feeRecipient`, `prevRandao`, `gasLimit`). `beaconRoot`, `withdrawals` and `blockHash` are rejected. In `trace_call` and `trace_callMany` a call with a zero effective gas price sees `BASEFEE` 0 even when the base fee is overridden. |
| `tracerConfig` | Object | all methods with trace settings | `{"includePrecompiles": true}` includes the precompile frames that are omitted by default (see [Precompile frames](#precompile-frames)). |
| `IncludeWithdrawals` | Boolean | `trace_block`, `trace_replayBlockTransactions` | Reports beacon-chain withdrawals; see [Beacon-chain withdrawals](#beacon-chain-withdrawals). |
| `tracer` | String | — | Not supported: a non-empty value returns an error. Use the `debug_trace*` methods for named or JavaScript tracers. |

Members of the object used by the `debug_trace*` methods, such as `timeout` or `noRefunds`, are accepted and ignored.

:::warning
State overrides must be wrapped in the trace settings object: `{"stateOverrides": {"0x…": {"balance": "0x…"}}}`. An address-keyed override map passed directly as the fourth parameter of `trace_call` (or the third of `trace_callMany`) is read as a trace settings object with no known members, so the overrides are silently ignored. The replay, block and filter methods also accept `stateOverrides` without applying it.
:::

## Precompile frames

All methods apply the same rule to calls to precompiled contracts:

* A transaction that calls a precompile directly always has its root frame.
* A nested call to a precompile is omitted when it carries no value, also when it fails a depth or balance check. `CALL` and `CALLCODE` use their value operand, `DELEGATECALL` the value it inherits from its caller, and `STATICCALL` always has zero value.
* A nested call to a precompile with a nonzero value, transferred or inherited, is kept, whether it succeeds or fails.

Omitted frames do not take a `traceAddress` index, which also determines the paths accepted by `trace_get` and the records that `trace_filter` can match. The trace settings `{"tracerConfig": {"includePrecompiles": true}}` include all precompile frames.

## Beacon-chain withdrawals

Beacon-chain withdrawals credit balances outside of any transaction, so by default no
trace mentions them. `trace_block` and `trace_replayBlockTransactions` can be asked to
include them by passing a trace-settings object as the last positional parameter. It is
positional, so the parameters before it have to be supplied even when they are not
otherwise needed:

```js
// trace_block(blockNumber, gasBailOut, traceSettings)
["0x1194bf0", false, { "IncludeWithdrawals": true }]

// trace_replayBlockTransactions(blockNumber, traceTypes, gasBailOut, traceSettings)
["0x1194bf0", ["stateDiff"], false, { "IncludeWithdrawals": true }]
```

The default is off — omit the object, or leave the field out, and withdrawals are not
reported. The member name is case-insensitive. `trace_filter` ignores it. How withdrawals
appear depends on the method:

* **`trace_block`** appends one entry per withdrawal to the flat trace list, after the
  block and uncle rewards. Each is a `"reward"` entry whose `action.rewardType` is
  `"withdrawal"`, with `action.author` set to the withdrawal address and `action.value`
  to the amount **in wei** (the beacon chain denominates withdrawals in Gwei). These
  entries carry `transactionHash` and `transactionPosition` as `null`, since no
  transaction caused them.

* **`trace_replayBlockTransactions`** has no block-level slot in its per-transaction
  result shape, so withdrawals surface only through `stateDiff`. You must request
  `"stateDiff"` in the trace types; when you do and the block has withdrawals, one extra
  entry is appended to the result array, carrying the withdrawal state changes. This entry
  has no `transactionHash`, its `output` is `"0x"`, its `trace` is `[]` and its `vmTrace`
  is `null`. Its `stateDiff` depends on whether the recipient already existed: an existing
  account gets a `"*"` balance change with `code` and `nonce` marked `"="`, while an
  account created by the withdrawal itself gets `"+"` entries for `balance`, `code`
  (`"0x"`) and `nonce` (`"0x0"`). Storage is empty either way. Multiple withdrawals to the
  same address are merged into a single balance change.

Withdrawal entries are an Erigon extension: other clients report one result per transaction and no withdrawal records.

## The gasBailOut option

`gasBailOut` relaxes the balance rules during replay. It is exposed as a parameter by
`trace_replayBlockTransactions`, `trace_replayTransaction`, `trace_block`,
`trace_transaction`, `trace_get` and `trace_filter`, defaulting to `false` in each.
`trace_call`, `trace_callMany` and `trace_rawTransaction` never enable it.
`trace_call` and `trace_callMany` run each call object with the fees and block
environment `eth_call` gives it: a call with a zero effective gas price runs free and sees `BASEFEE` 0,
and a priced call is checked and pays for its gas. A signed transaction passed to
`trace_rawTransaction` pays for its gas as it would in a block. The gas
is bought before execution, so `BALANCE(ORIGIN)`, or `SELFBALANCE` in a delegated sender,
reads the balance after that charge; this shows in `trace` and `vmTrace` as well as in
`stateDiff`.

:::warning
`gasBailOut` is not only a bypass for senders who cannot afford the gas charge. It
changes replayed state in ways that make the output unsuitable for reconstructing
balances:

* **Gas and blob fees are never deducted.** `TxnExecutor.buyGas` skips both `SubBalance`
  calls whenever the flag is set, for *every* replayed transaction — funded senders
  included, not only underfunded ones. The check that the sender can pay the upfront cost
  is skipped as well.
* **Refunds are skipped.** The gas-refund path is gated on `refunds && !gasBailout` — that
  is the internal Go parameter, spelled with a lowercase `o`, not the JSON-RPC
  `gasBailOut` this section documents. Before Amsterdam, the EIP-7623 calldata floor is
  not applied either.
* **The producer is paid on the gas used before refunds.** The block producer's tip is
  `gasUsed * effectiveTip`, and with the flag set `gasUsed` does not subtract the refund,
  so for a transaction with a refund the credit is larger than the canonical one.
* **An unaffordable value transfer is credited anyway.** When the sender cannot cover a
  non-zero `value`, the EVM-level bailout engages and `Transfer` skips the sender debit
  while still crediting the recipient. Value appears from nowhere, so a trace can show
  what looks like balance creation.
* **Where the chain configures a burn contract, that contract is credited too.** A
  **non-free** London transaction credits the configured `burntContract` with
  `gasUsed * baseFee`, using the same unrefunded `gasUsed`; on Aura from Prague the blob
  fee is added on top. Gnosis and Chiado are the chains that configure one. The sender
  was never debited, so this is a second source of apparent balance creation in
  `stateDiff`. Aura marks zero-fee certified service transactions as free and the credit
  is guarded by `!msg.IsFree()`, so those are excluded. Chains with no configured
  contract never receive this extra credit — every other effect above still applies to
  them.
:::

Whether one transaction's altered state is visible to the next depends on the execution
path. Replaying a block for traces alone, the latest block included, runs each
transaction in parallel against its own canonical pre-state, so nothing propagates. The
sequential path — taken when `stateDiff` or `vmTrace` is requested, and for
single-transaction blocks — carries the altered state forward to every later transaction
in the block. `trace_replayTransaction`, `trace_transaction` and `trace_get` replay only
the selected transaction on its canonical pre-state, so nothing propagates there.

## Errors

A failure inside the EVM, such as a revert or an out-of-gas halt, is reported in the trace output, not as an error. The JSON-RPC errors returned by the `trace_*` methods are:

| Code | Cause |
| --- | --- |
| `-32602` | Invalid parameters: a malformed value, a missing required parameter or too many parameters; an unknown trace type; the `pending` block tag; `data` and `input` both set and different; a `chainId` that does not match the chain; `maxPriorityFeePerGas` above `maxFeePerGas`; an integer element in a `trace_get` path; a malformed raw transaction; the `trace_filter` errors listed under [trace\_filter](#trace_filter). |
| `-32001` | The selected block is unknown: a block number above the head, a hash the node does not know, or a `safe` or `finalized` tag that cannot be resolved. Also a known hash that is not canonical, in `trace_replayBlockTransactions` and with `requireCanonical: true` in `trace_call` and `trace_callMany`. |
| `4444` | `trace_call`, `trace_callMany`: a known hash that is not canonical, without `requireCanonical`. The node keeps state only for canonical blocks. |
| `-38012` | `trace_call`, `trace_callMany`: the fee cap or gas price is below the block base fee. |
| `-38013` | `trace_call`, `trace_callMany`: the gas limit is below the intrinsic gas. |
| `-38014` | `trace_call`, `trace_callMany`: the sender cannot pay for the gas and the value. |
| `-38025` | `trace_call`, `trace_callMany`: the init code is larger than the limit. |
| `-38026` | `trace_rawTransaction`: the gas limit, or the size of the authorization list, exceeds `--rpc.gascap`. |
| `-32000` | Any other error, with a descriptive message. This includes: history outside the retention window (`old data not available due to pruning: … available from block N`); the validation errors of `trace_rawTransaction` (`nonce too low`, `nonce too high`, `insufficient funds …`, fee errors, a transaction signed for another chain); validation errors of a call object without a dedicated code (for example `gasPrice` together with `maxFeePerGas`, blob fields without blob hashes, an empty `authorizationList`, a feature not active at the selected block); a malformed `trace_callMany` item; an unsupported `blockOverrides` member; the `tracer` member; a timeout. |

## Configuration flags

| Flag | Default | Effect on the `trace_*` methods |
| --- | --- | --- |
| `--rpc.gascap` | `50000000` | Gas limit of `trace_call` and `trace_callMany` calls without `gas`; larger supplied limits are reduced to it. A `trace_rawTransaction` transaction above it is rejected (`-38026`). |
| `--rpc.evmtimeout` | `5m` | Time limit of an execution. `trace_call` and `trace_rawTransaction` abort the EVM (`execution aborted (timeout = …)`). `trace_callMany` and the replay methods check it between transactions. |
| `--rpc.blockrange.limit` | `1000` | Maximum `toBlock - fromBlock` of a `trace_filter` range; `0` means no limit. |
| `--trace.compat` | `false` | Bug-for-bug compatibility with OpenEthereum: the error of a root `create` frame is not reported, and `vmTrace` operations have no `op` member. |

`--trace.maxtraces` is accepted but currently has no effect: `trace_filter` returns all matching records unless `count` limits them.

## JSON-RPC methods

#### Ad-hoc Tracing

* [trace\_call](#trace_call)
* [trace\_callMany](#trace_callmany)
* [trace\_rawTransaction](#trace_rawtransaction)
* [trace\_replayBlockTransactions](#trace_replayblocktransactions)
* [trace\_replayTransaction](#trace_replaytransaction)

#### Transaction-Trace Filtering

* [trace\_block](#trace_block)
* [trace\_filter](#trace_filter)
* [trace\_get](#trace_get)
* [trace\_transaction](#trace_transaction)

## Response Fields Reference

All `trace_*` methods return objects built from the same set of fields. Each method's "Returns" section below indicates which top-level shape it produces; the field semantics are defined once here.

### Top-level shapes

| Method | Top-level shape |
| --- | --- |
| `trace_call`, `trace_rawTransaction` | `Object` with `output`, `stateDiff`, `trace[]`, `vmTrace` |
| `trace_replayTransaction` | `Object` with `output`, `stateDiff`, `trace[]`, `vmTrace`, `transactionHash`, or `null` for an unknown transaction |
| `trace_callMany` | `Array<Object>` — one per input call |
| `trace_replayBlockTransactions` | `Array<Object>` — one per transaction; each entry adds `transactionHash`. One extra trailing entry, without `transactionHash`, when `stateDiff` is requested with `"IncludeWithdrawals": true` and the block has withdrawals |
| `trace_block`, `trace_filter` | `Array<TraceEntry>` (flat list across all call frames, plus reward entries) |
| `trace_transaction` | `Array<TraceEntry>` (flat list of the transaction's call frames), or `null` for an unknown transaction |
| `trace_get` | `TraceEntry` (single call frame at the requested `traceAddress` path), or `null` |

### Top-level (ad-hoc tracing) fields

| Field | Type | Description |
| --- | --- | --- |
| `output` | DATA | Return data of the top-level call (`0x` if no data was returned). Always present, also when only `stateDiff` or no trace type is requested. For a contract creation it is the code returned by the init code. |
| `stateDiff` | Object \| null | Set when `"stateDiff"` is requested in the trace types array. Maps each account whose state changed to an object describing changes to `balance`, `nonce`, `code`, and per-key `storage` entries. See [State diff](#state-diff). `null` if not requested. |
| `trace` | Array of TraceEntry | Set when `"trace"` is requested. Flat list of call frames executed during the transaction. Empty array (never `null`) if not requested. See **TraceEntry fields** below. |
| `vmTrace` | Object \| null | Set when `"vmTrace"` is requested. Step-by-step EVM trace; see [VM trace](#vm-trace). When requested it is always an object, also for a call to an account without code or to a precompile. `null` if not requested. |
| `transactionHash` | DATA, 32 BYTES | (Only in `trace_replayTransaction` results and `trace_replayBlockTransactions` entries) Hash of the transaction this trace belongs to. |

### TraceEntry fields

A `TraceEntry` represents a single call frame (root call, internal call, contract creation, or self-destruct), or a reward.

| Field | Type | Description |
| --- | --- | --- |
| `action` | Object | The action that initiated this call frame. Shape depends on `type` (see **Action variants**). |
| `result` | Object \| null | The outcome of the action. `null` if the call frame failed other than by reverting. See **Result variants**. |
| `error` | String | (Optional) Present when the call frame failed, with a Parity-style label: `"Reverted"`, `"Out of gas"`, `"Bad instruction"`, `"Bad jump destination"`, `"Stack underflow"`, `"Out of stack"`, `"Mutable Call In Static Context"`, `"Built-in failed"` (a precompile failure), `"Out of bounds"`, `"Invalid code"` (created code starting with `0xEF`), `"Contract address collision"`, `"Nonce overflow"`, `"Insufficient balance for transfer"` or `"Max call depth exceeded"`. A code deposit failure, including code above the EIP-170 size limit, and init code above the EIP-3860 size limit are `"Out of gas"`; the invalid instruction `0xFE` is `"Bad instruction"`. Any other failure keeps its Go error text. For `"Reverted"`, `result` holds `gasUsed` and `output`, the revert data, for a `create` frame too; for other errors, `result` is `null`. |
| `subtraces` | Integer | Number of direct child call frames produced by this frame. Used together with `traceAddress` to reconstruct the call tree from a flat list. A JSON number, not a hex string. |
| `traceAddress` | Array of Integer | Path to this frame inside the call tree. Empty array `[]` for the root call; `[0]` is the first child of the root; `[1, 0]` is the first child of the second child of the root, etc. The elements are JSON numbers. |
| `type` | String | One of `"call"`, `"create"`, `"suicide"` (self-destruct), `"reward"` (a consensus payout rather than a call; see the `reward` action below). |
| `blockHash` | DATA, 32 BYTES | (Only in block-level methods: `trace_block`, `trace_filter`, `trace_transaction`, `trace_get`) Hash of the block containing the transaction. |
| `blockNumber` | Integer | (Block-level methods only) Number of the block containing the transaction, as a JSON number. |
| `transactionHash` | DATA, 32 BYTES | (Block-level methods only) Hash of the transaction containing this call frame. `null` for `"reward"` entries. |
| `transactionPosition` | Integer | (Block-level methods only) Index of the transaction within the block, as a JSON number. `null` for `"reward"` entries. |

Frames inside the results of the ad-hoc methods (`trace_call`, `trace_callMany`, `trace_rawTransaction`, `trace_replayTransaction`, `trace_replayBlockTransactions`) have no `blockHash`, `blockNumber`, `transactionHash` or `transactionPosition`.

A call or creation that fails its depth or balance check before running (or, for a creation, its nonce check) still emits a frame, with its `error`, `result: null` and no subtraces. From Amsterdam, a creation that fails these checks emits no frame.

### Action variants

The `action` object's shape depends on `type`:

**`type: "call"`**

| Field | Type | Description |
| --- | --- | --- |
| `callType` | String | `"call"`, `"callcode"`, `"delegatecall"`, or `"staticcall"`. |
| `from` | DATA, 20 BYTES | Caller address. For `delegatecall` and `callcode`, the address whose code is executing. |
| `to` | DATA, 20 BYTES | Target address. For `delegatecall` and `callcode`, the address of the code that is run. |
| `value` | QUANTITY | Wei value transferred. A `delegatecall` reports the value it inherits from its caller (no transfer takes place), a `staticcall` reports 0. |
| `gas` | QUANTITY | Gas available to this call frame. For the root call, the transaction gas limit minus the intrinsic gas (access list and authorization costs included). For a nested call, the gas forwarded to the child (at most all but 1/64 of the caller's remaining gas) plus the 2,300 stipend when value is transferred. |
| `input` | DATA | Call data (selector + arguments). |

**`type: "create"`**

| Field | Type | Description |
| --- | --- | --- |
| `from` | DATA, 20 BYTES | Address that initiated the creation. |
| `creationMethod` | String | How the contract was created: `"create"` or `"create2"`. A creation by a transaction is `"create"`. |
| `value` | QUANTITY | Wei value sent to the new contract. |
| `gas` | QUANTITY | Gas available to the creation: for the root, the transaction gas limit minus the intrinsic gas; for a nested creation, all but 1/64 of the caller's remaining gas. |
| `init` | DATA | Init bytecode (constructor + runtime). |

**`type: "suicide"`** (self-destruct)

| Field | Type | Description |
| --- | --- | --- |
| `address` | DATA, 20 BYTES | Address of the self-destructing contract. |
| `refundAddress` | DATA, 20 BYTES | Address that received the contract's remaining balance. |
| `balance` | QUANTITY | Wei amount transferred to `refundAddress`. |

The entry records the `SELFDESTRUCT` opcode and its transfer, not the deletion of the account: since EIP-6780 the account usually survives.

**`type: "reward"`** (in `trace_block` and `trace_filter`)

| Field | Type | Description |
| --- | --- | --- |
| `author` | DATA, 20 BYTES | Address receiving the reward: the block author, the uncle author, the beneficiary attributed by the block-reward contract for `"external"`, or the withdrawal recipient for `"withdrawal"`. |
| `value` | QUANTITY | Wei amount of the reward. |
| `rewardType` | String | `trace_filter` emits only `"block"` and `"uncle"`. `trace_block` also returns `"external"` for a block-reward-contract payout on Aura chains such as Gnosis, and `"withdrawal"` when `IncludeWithdrawals` is set — see [Beacon-chain withdrawals](#beacon-chain-withdrawals). `"emptyStep"` and `"unknown"` are defined but no current code path produces either. |

Reward entries follow all transaction entries of their block: the block reward first, then the uncle rewards in uncle order, then the withdrawals when requested. The genesis block has no reward entries, and on Ethereum no block after the Merge has any. `trace_filter` computes `"block"` and `"uncle"` rewards with the Ethash rules; on Aura chains use `trace_block` for reward data.

### Result variants

The `result` object's shape depends on `type`:

**`type: "call"`**

| Field | Type | Description |
| --- | --- | --- |
| `gasUsed` | QUANTITY | Execution gas used by this call frame, including the gas used by its child frames, before refunds. For the root call it does not include the intrinsic gas or the EIP-7623 floor, so it differs from the `gasUsed` of the receipt. |
| `output` | DATA | Return data of this call frame (`0x` if no data was returned). |

**`type: "create"`**

| Field | Type | Description |
| --- | --- | --- |
| `gasUsed` | QUANTITY | Execution gas used by the creation, including its child frames and the code deposit, before refunds. |
| `code` | DATA | Deployed runtime bytecode of the new contract (`0x` if empty). |
| `address` | DATA, 20 BYTES | Address of the newly deployed contract. |

A reverted `create` frame deploys no contract, so its `result` has the `call` shape, `gasUsed` and `output`, with the revert data as `output`. `trace_filter` does not match a failed `create` by the address it would have occupied.

**`type: "suicide"` and `type: "reward"`**

`result` is `null` — these actions have no return value.

### Amsterdam fields

From the Amsterdam fork (EIP-8037 two-dimensional gas), `gas` and `gasUsed` keep reporting execution gas only, and some objects carry state-gas members:

| Field | Type | Description |
| --- | --- | --- |
| `action.stateGasReservoir` | QUANTITY | (`create` actions) State gas available to the creation. |
| `result.stateGasUsed` | Signed QUANTITY | (`call` and `create` results) Net state gas used by the frame; it can be negative (for example `"-0x5"`). |
| `vmTrace` `ops[].stateGasCost` | Integer | State gas charged by the operation, when not zero. |
| `vmTrace` `ops[].ex.stateUsed` | Integer | State gas remaining after the operation, when not zero. |

### Frame transactions

For an EIP-8141 frame transaction, the trace has a synthetic root `call` frame with one child per frame. The root `gas` is the transaction gas limit and the root `gasUsed` is the `gasUsed` of the receipt; each child reports the gas of its frame receipt. A frame that was skipped has the error `"frame skipped"` and `result: null`. In `vmTrace`, the root has one operation per frame with code: its `pc` is the frame index and it has no `op`.

### State diff

`stateDiff` compares the state at the start and at the end of the execution. An account appears only if its balance, nonce, code, storage or existence changed: an account that was touched but left unchanged, or that was created and destroyed within the execution, is omitted. Each account object always has `balance`, `nonce`, `code` and `storage`:

| Marker | Meaning |
| --- | --- |
| `"="` | Field unchanged (`balance`, `nonce` and `code` of an account that exists at both ends). |
| `{"*": {"from": …, "to": …}}` | Field changed, for an account that exists at both ends. |
| `{"+": …}` | Account created: every field uses `+`, with the new value. |
| `{"-": …}` | Account deleted: every field uses `-`, with the old value. |

`storage` maps 32-byte slot keys to changes of 32-byte words. A slot of an existing account uses `*`, also when the new value is zero; a slot written with its original value is not listed. A created account lists its nonzero slots with `+`. Slots never use `=`. A deleted account has empty `storage`. An existing empty account (zero balance and nonce, no code) that the execution touches is removed under EIP-161, so it appears as deleted.

Writes reverted inside the EVM do not appear. The changes made at transaction level survive a revert: the sender's nonce and fee payment, the fee recipient's tip, and the code and nonce changes of accepted EIP-7702 authorizations, which are reported as their net effect. On Ethereum the base fee is burned, so it appears only as part of the sender's balance change; a `trace_call` with a zero gas price pays no fee at all. In `trace_callMany`, each `stateDiff` is relative to the state left by the previous call.

System operations of a block (the EIP-4788 and EIP-2935 calls before the transactions, the EIP-7002 and EIP-7251 calls after them), withdrawals and rewards belong to no transaction's `stateDiff`, so concatenating the diffs of a block does not reproduce its post-state.

### VM trace

A `vmTrace` object has:

* `code`: the bytecode actually executed — the init code for a creation, the resolved code for an EIP-7702 delegated account, `"0x"` when no code ran.
* `ops`: the operations that executed, in order. An operation rejected before execution (an undefined opcode, including the invalid instruction `0xFE`, a stack underflow or a stack overflow) is omitted, and no `STOP` is added at the end, so every `pc` lies inside `code`.

Each operation has these members:

| Member | Type | Description |
| --- | --- | --- |
| `pc` | Integer | Program counter. |
| `op` | String | Mnemonic, for example `"PUSH1"`. Absent when the node runs with `--trace.compat`. |
| `cost` | Integer | Gas charged when the operation starts, including the gas made available to a child frame (for the `CALL` family the forwarded gas without the 2,300 stipend, for the `CREATE` family the gas given to the child). |
| `ex` | Object \| null | Effects of the operation; `null` for an operation that halted with an error. |
| `ex.used` | Integer | Gas **remaining** after the operation (despite the name), including the gas returned by a child frame. |
| `ex.push` | Array | Stack words the operation leaves, deepest first, as hex quantities: one for most operations, `n+1` for `DUPn` and `SWAPn`, the result for `CALL` and `CREATE` after the child returns, none for operations that push nothing. |
| `ex.mem` | Object \| null | `{off, data}`: memory written or read by the operation — 32 bytes for `MSTORE` and `MLOAD`, 1 byte for `MSTORE8`, the destination of `CALLDATACOPY`, `CODECOPY`, `EXTCODECOPY`, `RETURNDATACOPY` and `MCOPY`, the full output window for the `CALL` family. `null` when the range is empty or the operation has none. `off` is a JSON number. |
| `ex.store` | Object \| null | `{key, val}` of an `SSTORE`, as hex quantities, also when the value does not change. `null` for every other operation. |
| `sub` | Object \| null | `vmTrace` of the child frame entered by the operation — `{"code": "0x", "ops": []}` for a precompile or an account without code. `null` when no frame was entered (`SELFDESTRUCT`, a call or creation that failed its depth or balance check, an operation that halted with an error). |

***

## JSON-RPC API Reference

The examples of the replay and filtering methods use old mainnet blocks and transactions. On a pruned node they return an error or `null`; run them on an archive node.

### trace\_call

Executes the given call and returns a number of possible traces for it.

The call runs on the state at the end of the selected block, with the block environment (`NUMBER`, `TIMESTAMP`, base fee, …) of that block. Simulated changes are discarded.

#### Parameters

1. `Object` - Call object. Supported fields: `from`, `to`, `gas`, `gasPrice`, `maxFeePerGas`, `maxPriorityFeePerGas`, `maxFeePerBlobGas`, `value`, `data` or `input`, `accessList`, `nonce`, `chainId`, `blobVersionedHashes`, `authorizationList`. All fields are optional, and `null` is the same as omitting a field. An omitted or `null` `to` creates a contract. `data` and `input` must be equal when both are set. `nonce` is accepted but neither checked nor used, so a creation derives its address from the sender's nonce in the state. `type` and unknown fields are ignored.
2. `Array` - Trace types: zero or more of `"vmTrace"`, `"trace"`, `"stateDiff"`.
3. `Quantity`, `Tag` or `Hash` - (optional) Block number, one of the tags `"latest"` (default), `"earliest"`, `"safe"`, `"finalized"`, or a block hash, as a string or as an EIP-1898 object (`{"blockHash": …}` or `{"blockNumber": …}`). `null` is the same as `"latest"`. A hash selects its block even if it is not canonical, but the node keeps no state for such a block, so the call returns `4444`; with `requireCanonical: true` it returns `-32001`. `"pending"` is not supported and returns `-32602`: the call is executed against committed state, so there is no pending block to execute on top of.
4. `Object` - (optional) Trace settings; see [Optional parameters and trace settings](#optional-parameters-and-trace-settings).

Fees and gas follow `eth_call`:

* With a zero effective gas price (no fee field, or all of them `0`) the call pays no fee and sees `GASPRICE` and `BASEFEE` 0; a blob call without `maxFeePerBlobGas` sees `BLOBBASEFEE` 0. The sender must still be able to transfer `value`.
* A priced call is validated against the block base fee and charged for its gas.
* Without `gas`, the call gets `--rpc.gascap` (50,000,000 by default); a larger `gas` is reduced to it. With a nonzero gas price the sender must be able to pay for that whole limit, so set `gas` explicitly. Unlike `eth_call`, an explicit `"gas": "0x0"` is a limit of zero and is rejected with `-38013`.

#### Returns

`Object` containing `output`, `stateDiff`, `trace[]`, `vmTrace`. Each requested trace type is populated; `stateDiff` and `vmTrace` are `null` when not requested, while `trace` is an empty array. See [Response Fields Reference](#response-fields-reference) for full field semantics.

#### Example

Request

```bash
curl --data '{"method":"trace_call","params":[{ ... },["trace"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "output": "0x",
    "stateDiff": null,
    "trace": [{
      "action": { ... },
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "type": "call"
    }],
    "vmTrace": null
  }
}
```

With a state override, wrapped in the trace settings:

```bash
curl --data '{"method":"trace_call","params":[{"from":"0x28c6c06298d514db089934071355e5743bf21d60","to":"0x000000000000000000000000000000000000dead","value":"0x1"},["stateDiff"],"latest",{"stateOverrides":{"0x28c6c06298d514db089934071355e5743bf21d60":{"balance":"0x100"}}}],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```

***

### trace\_callMany

Performs multiple call traces on top of the same block. i.e. transaction `n` will be executed on top of the selected block's state with all `n-1` transactions applied (traced) first. Allows to trace dependent transactions.

All calls share the block environment of the selected block, so `NUMBER` and `TIMESTAMP` do not advance between them. Each call follows the rules of [trace\_call](#trace_call). The state overrides of the trace settings are applied once, before the first call. An empty array returns `[]`.

If a call fails validation, the whole request returns one error and no results. The message gives the zero-based index of the failing call (`first run for txIndex N error: …` or `call N: …`); errors found while decoding or converting a call object do not include it.

#### Parameters

1. `Array` - List of `[call, traceTypes]` pairs: a call object as in `trace_call` and its trace types, zero or more of `"vmTrace"`, `"trace"`, `"stateDiff"`.
2. `Quantity`, `Tag` or `Hash` - (optional) Block selector as in `trace_call`, default `"latest"`. `'pending'` is not supported and returns `-32602`: the calls are executed against committed state, so there is no pending block to execute on top of.
3. `Object` - (optional) Trace settings; see [Optional parameters and trace settings](#optional-parameters-and-trace-settings).

```js
params: [
  [
    [
      {
        "from": "0x28c6c06298d514db089934071355e5743bf21d60",
        "to": "0x000000000000000000000000000000000000dead",
        "value": "0x186a0"
      },
      ["trace"]
    ],
    [
      {
        "from": "0x28c6c06298d514db089934071355e5743bf21d60",
        "to": "0x000000000000000000000000000000000000dead",
        "value": "0x186a0"
      },
      ["trace"]
    ]
  ],
  "latest"
]
```

#### Returns

`Array<Object>` — one entry per input call, each shaped like the `trace_call` response (`output`, `stateDiff`, `trace[]`, `vmTrace`). Each `stateDiff` is relative to the state left by the previous call. See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_callMany","params":[[[{"from":"0x28c6c06298d514db089934071355e5743bf21d60","to":"0x000000000000000000000000000000000000dead","value":"0x186a0"},["trace"]],[{"from":"0x28c6c06298d514db089934071355e5743bf21d60","to":"0x000000000000000000000000000000000000dead","value":"0x186a0"},["trace"]]],"latest"],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "output": "0x",
      "stateDiff": null,
      "trace": [{
        "action": {
          "callType": "call",
          "from": "0x28c6c06298d514db089934071355e5743bf21d60",
          "gas": "0x2fa9e78",
          "input": "0x",
          "to": "0x000000000000000000000000000000000000dead",
          "value": "0x186a0"
        },
        "result": {
          "gasUsed": "0x0",
          "output": "0x"
        },
        "subtraces": 0,
        "traceAddress": [],
        "type": "call"
      }],
      "vmTrace": null
    },
    {
      "output": "0x",
      "stateDiff": null,
      "trace": [{
        "action": {
          "callType": "call",
          "from": "0x28c6c06298d514db089934071355e5743bf21d60",
          "gas": "0x2fa9e78",
          "input": "0x",
          "to": "0x000000000000000000000000000000000000dead",
          "value": "0x186a0"
        },
        "result": {
          "gasUsed": "0x0",
          "output": "0x"
        },
        "subtraces": 0,
        "traceAddress": [],
        "type": "call"
      }],
      "vmTrace": null
    }
  ]
}
```

***

### trace\_rawTransaction

Traces a call to `eth_sendRawTransaction` without making the call, returning the traces.

The transaction runs on the latest state, with the block environment of the latest block. It is validated as it would be for inclusion in a block: the signature and chain ID, a nonce equal to the sender's nonce (too low and too high are both rejected), the balance for the value and the gas, the intrinsic gas, the fees, the sender-code rule of EIP-3607 (with the EIP-7702 delegation exception) and the gas limit cap of EIP-7825. A transaction that fails validation returns an error; a valid transaction that reverts or halts returns its trace. The sender pays for the gas, and `gasBailOut` is not available.

#### Parameters

1. `Data` - Raw signed transaction.
2. `Array` - Trace types: zero or more of `"vmTrace"`, `"trace"`, `"stateDiff"`.

A block selector is not accepted.

```js
params: [
  "0xf86a80847735940082520894000000000000000000000000000000000000123401808718e5bb3abd10a0a04c833abdb116ad76fc98610ae0e626d7e35154f1fba38ce6bfdaf69a31eec42ca035ecfb996cc68a2a03df0e1087194e25180b01bf0469f69b4f1d330eea9d027a",
  ["trace"]
]
```


#### Returns

`Object` shaped like the `trace_call` response (`output`, `stateDiff`, `trace[]`, `vmTrace`). See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_rawTransaction","params":["0xf86a80847735940082520894000000000000000000000000000000000000123401808718e5bb3abd10a0a04c833abdb116ad76fc98610ae0e626d7e35154f1fba38ce6bfdaf69a31eec42ca035ecfb996cc68a2a03df0e1087194e25180b01bf0469f69b4f1d330eea9d027a",["trace"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


The transaction above is signed for a test chain, so on Ethereum mainnet it is rejected with `{"code": -32000, "message": "invalid chain id for signer"}`. A transaction that is valid at the latest state returns a response with this shape:

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "output": "0x",
    "stateDiff": null,
    "trace": [{
      "action": { ... },
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "type": "call"
    }],
    "vmTrace": null
  }
}
```

***

### trace\_replayBlockTransactions

Replays all transactions in a block returning the requested traces for each transaction.

#### Parameters

1. `Quantity`, `Tag` or `Hash` - Block number, one of the tags `"latest"`, `"earliest"`, `"safe"`, `"finalized"`, or the hash of a canonical block, as a string or as an EIP-1898 object. `"earliest"` is block 0, which is outside the retention window of a pruned node. `'pending'` is not supported and returns `-32602`: tracing replays committed state, so there is no pending block to replay.
2. `Array` - Trace types: zero or more of `"vmTrace"`, `"trace"`, `"stateDiff"`.
3. `Boolean` - (optional) `gasBailOut`, default `false`.
4. `Object` - (optional) Trace settings; see [Optional parameters and trace settings](#optional-parameters-and-trace-settings).

```js
params: [
  "0x2ed119",
  ["trace"]
]
```

#### Returns

`Array<Object>` — one entry per transaction in the block. Each entry is shaped like the `trace_call` response plus a `transactionHash` field identifying the transaction. See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_replayBlockTransactions","params":["0x2ed119",["trace"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "output": "0x",
      "stateDiff": null,
      "trace": [{
        "action": { ... },
        "result": {
          "gasUsed": "0x0",
          "output": "0x"
        },
        "subtraces": 0,
        "traceAddress": [],
        "type": "call"
      }],
      "transactionHash": "0x...",
      "vmTrace": null
    },
    { ... }
  ]
}
```

***

### trace\_replayTransaction

Replays a transaction, returning the traces.

#### Parameters

1. `Hash` - Transaction hash.
2. `Array` - Trace types: zero or more of `"vmTrace"`, `"trace"`, `"stateDiff"`.
3. `Boolean` - (optional) `gasBailOut`, default `false`.
4. `Object` - (optional) Trace settings; see [Optional parameters and trace settings](#optional-parameters-and-trace-settings).

```js
params: [
  "0x02d4a872e096445e80d05276ee756cefef7f3b376bcec14246469c0cd97dad8f",
  ["trace"]
]
```

#### Returns

`Object` shaped like the `trace_call` response (`output`, `stateDiff`, `trace[]`, `vmTrace`) plus a `transactionHash` field identifying the replayed transaction. See [Response Fields Reference](#response-fields-reference).

`null` if the transaction is not in a canonical block. On a pruned node, a transaction whose lookup entry has been pruned also returns `null`; a transaction that is found but whose state history has been pruned returns an error.

#### Example

Request

```bash
curl --data '{"method":"trace_replayTransaction","params":["0x02d4a872e096445e80d05276ee756cefef7f3b376bcec14246469c0cd97dad8f",["trace"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "output": "0x",
    "stateDiff": null,
    "trace": [{
      "action": { ... },
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "type": "call"
    }],
    "transactionHash": "0x02d4a872e096445e80d05276ee756cefef7f3b376bcec14246469c0cd97dad8f",
    "vmTrace": null
  }
}
```

***

### trace\_block

Returns traces created at the given block.

#### Parameters

1. `Quantity` or `Tag` - Block number, or one of the tags `"latest"`, `"earliest"`, `"safe"`, `"finalized"`, `"latestExecuted"`. A block hash is not accepted. `'pending'` is not supported and returns `-32602`: tracing replays committed state, so there is no pending block to replay.
2. `Boolean` - (optional) `gasBailOut`, default `false`.
3. `Object` - (optional) Trace settings; see [Optional parameters and trace settings](#optional-parameters-and-trace-settings).

```js
params: [
  "0x2ed119" // 3068185
]
```

#### Returns

`Array<TraceEntry>` — flat list of all call frames in the block, followed by any `"reward"` entries. Each entry includes block-level fields (`blockHash`, `blockNumber`, `transactionHash`, `transactionPosition`). The genesis block returns `[]`. See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_block","params":["0x2ed119"],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "action": {
        "callType": "call",
        "from": "0xaa7b131dc60b80d3cf5e59b5a21a666aa039c951",
        "gas": "0x0",
        "input": "0x",
        "to": "0xd40aba8166a212d6892125f079c33e6f5ca19814",
        "value": "0x4768d7effc3fbe"
      },
      "blockHash": "0x7eb25504e4c202cf3d62fd585d3e238f592c780cca82dacb2ed3cb5b38883add",
      "blockNumber": 3068185,
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "transactionHash": "0x07da28d752aba3b9dd7060005e554719c6205c8a3aea358599fc9b245c52f1f6",
      "transactionPosition": 0,
      "type": "call"
    },
    ...
  ]
}
```

***

### trace\_filter

Returns traces matching the given filter.

#### Parameters

1. `Object` - The filter object
   * `fromBlock`: `Quantity` or `Tag` - (optional) From this block. Defaults to the latest executed block; to search history, set it explicitly.
   * `toBlock`: `Quantity` or `Tag` - (optional) To this block. Defaults to the latest executed block. A `toBlock` below `fromBlock`, including the default start, returns `-32602`.
   * `blockHash`: `Data`, 32 Bytes - (optional) Selects exactly the block with this hash, as in `eth_getLogs`, instead of a range. The result, including `[]`, is for that block, and address matching, `mode`, `after` and `count` apply as for a single-block range. Setting it together with `fromBlock` or `toBlock` returns `-32602`. A hash that does not name a canonical, executed block, including one reorganized out of the chain, returns `-32001`, even with `count` 0; a block outside the retention window returns an error naming the first available block (`-32000`).
   * `fromAddress`: `Array` - (optional) Sent from these addresses.
   * `toAddress`: `Array` - (optional) Sent to these addresses.
   * `after`: `Integer` - (optional) Number of matching records to skip. A JSON number; a hex string returns `-32602`.
   * `count`: `Integer` - (optional) Maximum number of records to return. A JSON number; a hex string returns `-32602`. Without `count`, all matching records are returned. `count: 0` returns `[]` after the other members have been validated.
   * `mode`: `String` - (optional) Default is `"intersection"`: OR within each address list, AND between the two lists. An omitted, `null`, or empty list imposes no restriction. Set `"union"` to match either populated list and preserve the previous behavior when both lists are set. A `null` mode is the same as an omitted one. Other mode values, including `""` and values in a different case such as `"Union"`, return `-32602`.

   The bounds accept a block number or the tags `"earliest"`, `"latest"`, `"safe"`, `"finalized"` and `"latestExecuted"`. `"earliest"` is block 0, so on a pruned node it returns the pruning error; use a block number inside the retention window instead. The `'pending'` tag is not supported for either block bound and returns `-32602`: `trace_filter` scans committed trace history, which has no pending block.

   A bound past the latest executed block returns `-32602`, as in `eth_getLogs`, rather than an empty or truncated result. As in `eth_getLogs`, a bound is a block number or tag: a block hash or an EIP-1898 object returns `-32602`; use `blockHash` to select a block by hash. An explicit `null` for `blockHash`, `fromBlock` or `toBlock` is the same as omitting it.

   A range wider than `--rpc.blockrange.limit` (1,000 blocks by default) returns `-32602`, as in `eth_getLogs`. An unknown member, for example a misspelled one, returns `-32602`; member names are matched case-insensitively.
2. `Boolean` - (optional) `gasBailOut`, default `false`.
3. `Object` - (optional) Trace settings; see [Optional parameters and trace settings](#optional-parameters-and-trace-settings).

Matching and order:

* A `call` matches its `from` against `fromAddress` and its `to` against `toAddress`; a `create` its creator and the created address; a `suicide` the self-destructing contract and its `refundAddress`. Addresses are compared regardless of letter case.
* A `reward` entry is returned when no address list is set, or when its `author` is in `toAddress`.
* Records are ordered by block, then by transaction, then in call-tree order, with the rewards of each block after its transactions. `after` and `count` apply after the filter.

```js
params: [{
  "fromBlock": "0x2ed0c4", // 3068100
  "toBlock": "0x2ed128", // 3068200
  "toAddress": ["0x8bbB73BCB5d553B5A556358d27625323Fd781D37"],
  "after": 1000,
  "count": 100
}]
```

#### Returns

`Array<TraceEntry>` — flat list of call frames that match the filter, including any matching `"reward"` entries. Each entry has block-level fields (`blockHash`, `blockNumber`, `transactionHash`, `transactionPosition`). See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_filter","params":[{"fromBlock":"0x2ed0c4","toBlock":"0x2ed128","toAddress":["0x8bbB73BCB5d553B5A556358d27625323Fd781D37"],"after":1000,"count":100}],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "action": {
        "callType": "call",
        "from": "0x32be343b94f860124dc4fee278fdcbd38c102d88",
        "gas": "0x4c40d",
        "input": "0x",
        "to": "0x8bbb73bcb5d553b5a556358d27625323fd781d37",
        "value": "0x3f0650ec47fd240000"
      },
      "blockHash": "0x86df301bcdd8248d982dbf039f09faf792684e1aeee99d5b58b77d620008b80f",
      "blockNumber": 3068183,
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "transactionHash": "0x3321a7708b1083130bd78da0d62ead9f6683033231617c9d268e2c7e3fa6c104",
      "transactionPosition": 3,
      "type": "call"
    },
    ...
  ]
}
```

***

### trace\_get

Returns the call frame at the given `traceAddress` path.

#### Parameters

1. `Hash` - Transaction hash.
2. `Array` - `traceAddress` path of the call frame, as hex quantities: `[]` for the root call, `["0x1", "0x0"]` for the first child of the second child of the root. The `traceAddress` returned in traces uses JSON numbers, so convert each element to a hex string; an integer element returns `-32602`.
3. `Boolean` - (optional) `gasBailOut`, default `false`.
4. `Object` - (optional) Trace settings; see [Optional parameters and trace settings](#optional-parameters-and-trace-settings).

```js
params: [
  "0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",
  ["0x0"]
]
```

#### Returns

A single `TraceEntry` corresponding to the call frame at the requested `traceAddress` path, with block-level fields (`blockHash`, `blockNumber`, `transactionHash`, `transactionPosition`). `null` if the transaction is not in a canonical block or the path does not exist. See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_get","params":["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",["0x0"]],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "action": {
      "callType": "call",
      "from": "0x1c39ba39e4735cb65978d4db400ddd70a72dc750",
      "gas": "0x13e99",
      "input": "0x16c72721",
      "to": "0x2bd2326c993dfaef84f696526064ff22eba5b362",
      "value": "0x0"
    },
    "blockHash": "0x7eb25504e4c202cf3d62fd585d3e238f592c780cca82dacb2ed3cb5b38883add",
    "blockNumber": 3068185,
    "result": {
      "gasUsed": "0x183",
      "output": "0x0000000000000000000000000000000000000000000000000000000000000001"
    },
    "subtraces": 0,
    "traceAddress": [
      0
    ],
    "transactionHash": "0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",
    "transactionPosition": 2,
    "type": "call"
  }
}
```

***

### trace\_transaction

Returns all traces of the given transaction.

#### Parameters

1. `Hash` - Transaction hash
2. `Boolean` - (optional) `gasBailOut`, default `false`.
3. `Object` - (optional) Trace settings; see [Optional parameters and trace settings](#optional-parameters-and-trace-settings).

```js
params: ["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3"]
```


#### Returns

`Array<TraceEntry>` — flat list of all call frames in the transaction, with block-level fields (`blockHash`, `blockNumber`, `transactionHash`, `transactionPosition`) on each entry. `null` if the transaction is not in a canonical block; on a pruned node, also when its lookup entry has been pruned. See [Response Fields Reference](#response-fields-reference).

#### Example

Request

```bash
curl --data '{"method":"trace_transaction","params":["0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3"],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```


Response

```js
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "action": {
        "callType": "call",
        "from": "0x1c39ba39e4735cb65978d4db400ddd70a72dc750",
        "gas": "0x13e99",
        "input": "0x16c72721",
        "to": "0x2bd2326c993dfaef84f696526064ff22eba5b362",
        "value": "0x0"
      },
      "blockHash": "0x7eb25504e4c202cf3d62fd585d3e238f592c780cca82dacb2ed3cb5b38883add",
      "blockNumber": 3068185,
      "result": {
        "gasUsed": "0x183",
        "output": "0x0000000000000000000000000000000000000000000000000000000000000001"
      },
      "subtraces": 0,
      "traceAddress": [
        0
      ],
      "transactionHash": "0x17104ac9d3312d8c136b7f44d4b8b47852618065ebfa534bd2d3b5ef218ca1f3",
      "transactionPosition": 2,
      "type": "call"
    },
    ...
  ]
}
```
