---
description: >-
  Definitions of technical terms related to Erigon, Ethereum, and blockchain
  technology.
---

# Glossary of Key Terms

This glossary provides concise definitions for essential terms related to the Erigon client. Understanding this terminology is crucial for troubleshooting, configuration, and general operation.

* **Account:** A unique entity on the Ethereum blockchain that can hold an ETH balance and send transactions. There are two types: externally owned accounts (EOAs) and contract accounts.
* **Archive Node:** A node that stores the complete history of the blockchain. This includes all historical states and past transactions, allowing for deep queries into any moment in the blockchain's history. Erigon is particularly known for its highly optimized archive node.
* **Attestation:** A validator's vote on the validity of a block or chain. A high attestation effectiveness is critical for a healthy validator and node.
* **Block:** A collection of transactions, data, and a header that is cryptographically linked to the previous block.
* **Consensus Client (CL):** Software that runs the Proof-of-Stake (PoS) consensus protocol. It is responsible for a node's peer discovery, block propagation, and attesting to blocks. Examples include Lighthouse, Nimbus, and Prysm.
* **datadir:** The directory where an Erigon node stores all of its blockchain data, including the database, snapshots, and temporary files.
* **Engine API:** The communication protocol that allows the **Execution Client** (Erigon) and the **Consensus Client** to communicate and exchange data, such as new blocks and validator attestations.
* **Erigon:** An Ethereum **Execution Client** built for efficiency. It is designed to be highly scalable and fast, with a focus on minimizing disk space and improving synchronization speed.
* **Execution Client (EL):** Software that executes and validates all transactions, and propagates new blocks across the network. It maintains a full record of the blockchain state. Erigon is an execution client.
* **Full Node:** A node that holds a complete copy of all block data, from the genesis block to the current head. It retains recent state, all blocks post-Merge, and prunes ancient blocks and state (EIP-4444 enabled). It verifies every block and state transition..
* **Go (Golang)**: The open-source programming language used to develop Erigon, known for its performance, concurrency, and efficiency.
* **Gnosis Chain:** A stable, community-owned EVM-compatible chain that uses a PoS consensus mechanism. Erigon has specific optimizations and troubleshooting steps for Gnosis Chain due to its large transaction history.
* **JSON RPC**: JSON Remote Procedure Call. A lightweight protocol used by Ethereum clients to communicate with applications (like wallets or block explorers) over HTTP or WebSockets.
* **head:** The most recent block in the blockchain.
* **Minimal Node**: A node that retains the minimum amount of data necessary to function, typically by heavily pruning historical state data to significantly save disk space.
* **MDBX:** The high-performance, key-value database that Erigon uses to store blockchain data. It is a more efficient and scalable alternative to the databases used by other clients.
* **Merkle Patricia Trie:** A data structure used by most Ethereum clients (like Geth) to store the blockchain state. It is highly secure but can be less space-efficient than MDBX.
* **Mempool:** A pool of unconfirmed transactions that have been submitted to the network but have not yet been included in a block.
* **Node:** A piece of software that runs on a computer and interacts with the blockchain network. It can be a full node, light node, or validator node.
* **OOM-kill:** An event where the operating system's "Out of Memory" killer terminates a process (e.g., Erigon) that is consuming too much memory.
* **Peer:** Another node on the network that your client is connected to. The more healthy peers you have, the more reliable your connection is.
* **Pruning:** The process of removing older, unnecessary data from the blockchain to save disk space. Erigon offers different pruning modes (full, minimal, archive) to suit various needs.
* **rpcdaemon:** A separate, lightweight process in Erigon that handles JSON RPC API requests. This design allows Erigon to continue syncing efficiently even under heavy RPC load.
* **Snapshot Sync:** A rapid synchronization method that downloads a pre-made snapshot of the blockchain state and then syncs the remaining blocks. This is much faster than syncing from the genesis block.
* **Staged Sync:** Erigon's unique synchronization model. It processes the blockchain in a series of logical stages, such as downloading headers, verifying bodies, and building the state, to maximize speed and efficiency.
* **Validator:** A participant in a Proof-of-Stake network who has staked the network token and is responsible for proposing and attesting to new blocks.
