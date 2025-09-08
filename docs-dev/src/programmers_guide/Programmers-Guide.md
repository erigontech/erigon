# Programmers Guide

## Development

Erigon is an open-source project that welcomes contributions from developers worldwide who are passionate about advancing the Ethereum ecosystem. Bounties may be offered for noteworthy contributions, as the team is committed to continuously enhancing the tool to better serve the Erigon community.

## Programmer's Guide

Begin by exploring the comprehensive **[Programmer's Guide](programmers_guide/guide.md)**, which covers topics such as the Ethereum state structure, account contents, and account addressing mechanisms. This guide serves as a valuable resource, providing detailed information on Erigon's architecture, coding conventions, and development workflows related to managing and interacting with the Ethereum state.

## Dive Deeper into the Architecture

For those interested in gaining a deeper understanding of Erigon's underlying architecture, visit the following resources:
- **[DB Walk-through](db_walkthrough.MD)**: This document provides a detailed walk-through of Erigon's database structure. It explains how Erigon organizes persistent data into tables like PlainState for accounts and storage, History Of Accounts for tracking account changes, and Change Sets for optimized binary searches on changes. It contrasts Erigon's approach with go-ethereum's use of the Merkle Patricia Trie.
- **[Database FAQ](db_faq.md)**: The Database FAQ addresses common questions and concerns related to Erigon's database design. It covers how to directly read the database via gRPC or while Erigon is running, details on the MDBX storage engine and RAM usage model, and points to further resources on the database interface rationale and architecture.

## Feature Exploration

Erigon introduces several innovative features that contributors may find interesting to explore and contribute to:
- **[DupSort Feature Explanation](dupsort.md)**: Erigon's DupSort feature optimizes storage and retrieval of duplicate data by utilizing prefixes for keys in databases without the concept of "Buckets/Tables/Collections" or by creating tables for efficient storage with named "Buckets/Tables/Collections."
- **[EVM without Opcodes](evm_semantics.md)** (Ether Transfers Only): Erigon explores a simplified version of the Ethereum Virtual Machine (EVM) focusing solely on ether transfers, offering an efficient execution environment for specific use cases.
