# Why using Erigon?

Erigon is an efficient Ethereum implementation designed for speed, modularity, and optimization. It achieves this by utilizing technologies like staged sync, efficient state storage, and database compression.

With Erigon 3, the default configuration is changing from an archive node to a **full node**. This shift enhances efficiency, accessibility, and versatility for a wider range of users, as the full node offers faster sync times and lower resource usage for everyday operations. Archive nodes—which retain full historical data—will still be available for developers and researchers who need them.

## Key Architectural Benefits

* **Staged Sync:** Erigon uses a unique staged synchronization process that breaks down the blockchain sync into a series of distinct, optimized stages (e.g., downloading headers, executing blocks, building the state). This approach allows Erigon to be faster and more efficient than traditional sync methods because it procesfses data in a way that minimizes database writes and resource usage. This results in significantly faster initial sync times, often in a matter of hours or days, making it far more accessible for users with standard hardware.
* **Modularity:** Erigon's architecture is highly modular, separating components like the core node, RPC daemon, and transaction pool into independent processes. This design enables RPC providers to create scalable, high-performance clusters by running multiple RPC daemons connected to a single node. For developers, this modularity simplifies development, allowing for easier integration of new features and debugging without affecting the entire system.
* **Efficient state Storage:** Erigon's approach to state storage is a core reason for its efficiency. Instead of storing data using the complex Merkle Patricia Trie (MPT) structure like most clients, Erigon uses a flat key-value database. This design simplifies data organization, reducing the need for multiple lookups and random disk access. This architecture not only speeds up read/write operations but also enables powerful data compression, which dramatically shrinks the overall disk footprint of an Erigon node. By making a full archive node feasible on consumer-grade SSDs, Erigon significantly lowers the hardware barrier for solo stakers, developers, and professionals.
* **BitTorrent Solution**: A major advantage of Erigon is its BitTorrent solution, providing immutable historical data with efficient re-sync and node repair capabilities through decentralized snapshots.
* **OtterSync:** OtterSync is a new sync algorithm designed to drastically reduce the time needed to sync a node. It achieves this by shifting most of the initial sync computation from the CPU to network bandwidth. This is done by downloading state data through a secondary protocol, like BitTorrent, instead of re-executing every historical transaction. This enables a fast sync, even for archive nodes.

## Advantages of using Erigon

### Professionals: RPC Providers and Stakers

Professionals, particularly **RPC providers** can leverage Erigon's unique architecture for enhanced performance and scalability. The client's modular design allows for the RPC daemon to be run as a separate process from the core node, which enables RPC providers to build scalable clusters by connecting multiple RPC daemons to a single node. This separation of concerns allows for flexible resource allocation and can handle a high volume of requests with low latency.

For **large-scale stakers**, Erigon's optimized performance, including fast block processing and an integrated consensus layer client (Caplin), reduces the risk of missed attestations and slashing penalties, leading to higher profitability and improved reliability.

#### Performance and Efficiency

* Faster sync and smaller disk footprint compared to other execution clients.
* Cheaper infrastructure due to a significantly smaller database size.
* The minimized disk footprint optimizes performance and reduces overall costs.
* Superior RPC performance, leading to a much-improved user experience.
* Faster RPC speeds, benefiting wallets and analytics tools.
* Quicker transaction broadcasting improves the end-user experience, preventing missed opportunities and reducing errors.

### Home Users and Solo Stakers

For home users and solo stakers, Erigon offers a compelling solution that prioritizes **efficiency and accessibility**. Its primary benefit lies in its reduced disk space requirements. By employing an innovative "flat" database schema and advanced data compression techniques, Erigon can store a full archive node in significantly less space than other clients. This makes it feasible for individuals to run a full node on consumer-grade hardware, lowering the barrier to entry for solo staking. Furthermore, Erigon's fast synchronization capabilities mean that a new node can be brought up to speed much quicker, reducing the initial setup time and allowing stakers to start earning rewards with minimal delay.

By running their own Erigon node, home users and solo stakers contribute directly to the **decentralization** and overall health of the Ethereum network. This practice reduces their dependence on third-party servers and the need to trust information about the state of the network provided by external parties. This self-reliance is a key aspect of the decentralization revolution, as it shifts power away from centralized entities and back to the individual user. By participating in this process, users not only improve their own **security and privacy** but also play a vital role in ensuring the long-term resilience and censorship resistance of the entire network.

### Developers

Erigon provides a robust and flexible environment for developers building on Ethereum and L2s. Its modular architecture simplifies development, allowing teams to focus on specific components without being burdened by the entire system. Developers can easily integrate new features or fix bugs within a specific module, enhancing development efficiency.

Erigon's full node capabilities by default, coupled with its fast sync, make it an ideal choice for building dApps, analytics platforms, or other applications that require access to comprehensive historical data. The client also offers a gRPC API for lower-level data access and a range of debug and tracing functionalities, empowering developers to create, test, and optimize their applications with greater ease.
