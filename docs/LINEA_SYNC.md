# Linea historical sync with an external consensus client

Custom chains with Clique history and a terminal total difficulty can start
the execution module before the transition. Their genesis configuration must
retain its `clique` object and the original terminal total difficulty.

Run Erigon with the initialized custom genesis, execution-layer peers that
serve the historical blocks, and an external consensus client such as Maru
connected to the authenticated Engine API (`--externalcl`). The two clients
must agree on the chain configuration.

The external client's fork-choice update supplies the target head hash.
Erigon's existing Engine API downloader fetches missing ancestry over DevP2P,
then imports and executes it in batches. Clique verifies the historical
signatures, signer votes, timestamps, and difficulty; the merge wrapper selects
PoS validation after terminal total difficulty. Head, safe, and finalized
hashes continue to come from the external client.

This does not restore autonomous Clique tip discovery or block production.
Bootnodes alone do not trigger sync. A recent `forkchoiceUpdated` request with
an unknown head triggers the historical download; `newPayload` recovery has a
bounded chain-length limit and is not a replacement for that initial request.

Use a fresh datadir for genesis-sync verification. An unpatched version that
initialized the datadir may already have discarded the `clique` configuration.
Do not change terminal total difficulty or force `terminalTotalDifficultyPassed`
to bypass historical validation.

EIP-7002 and EIP-7251 dequeue calls return no requests when their configured
contract address has no code. This behavior applies to all chains using these
functions; state-read and system-call errors still propagate.

## Validation

The patch includes regressions for Clique JSON persistence, engine selection,
and execution startup before the transition. Restored Clique voting tests cover
signer changes and invalid signatures. A synthetic import test executes signed
Clique blocks through terminal difficulty and imports the first PoS block.

Live verification should connect Maru to a fresh Erigon datadir, confirm an
initial fork-choice download, observe execution passing the terminal block,
and confirm continued head advancement with Maru's safe and finalized hashes.
This live check is separate from the offline regression suite.
