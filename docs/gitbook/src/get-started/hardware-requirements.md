---
description: 'Erigon Hardware Requirements: Disk Type and Size, RAM, and CPU for Node Types'
---

# Hardware Requirements

## Overview

A locally mounted **SSD** (Solid-State Drive) or **NVMe** (Non-Volatile Memory Express) disk is essential for optimal performance. Avoid Hard Disk Drives (HDD), as they can cause Erigon to lag behind the blockchain tip, albeit not fall behind.

<table data-header-hidden><thead><tr><th width="153.4444580078125"></th><th width="239.2222900390625"></th><th></th></tr></thead><tbody><tr><td><strong>Component</strong></td><td><strong>Recommendation</strong></td><td><strong>Notes</strong></td></tr><tr><td><strong>Disk Type</strong></td><td>Use high-end <strong>NVMe SSDs</strong>.</td><td><p>Avoid HDDs.</p><p>SSD performance may degrade when nearing full capacity.</p></td></tr><tr><td><strong>Disk Configuration</strong></td><td><strong>RAID 0</strong> for multiple disks (Speed)</td><td><p><strong>High-Speed Option</strong>: RAID 0 provides maximum speed but no redundancy (data loss risk).</p><p><strong>ZFS filesystems</strong> may be considered for Archive nodes for their data integrity features, but <strong>complex RAID-Z setups</strong> are generally <strong>not recommended</strong>.</p></td></tr><tr><td><strong>RAM</strong></td><td><strong>Adequate memory is crucial</strong></td><td>Reduces bottlenecks during sync and improves performance under load.</td></tr><tr><td><strong>CPU</strong></td><td><strong>4–8 cores</strong> for Full nodes<br><strong>8–16 cores</strong> for Archive nodes</td><td>More cores are generally better for intense sync and query operations.</td></tr><tr><td><strong>Linux</strong></td><td><strong>Kernel version > v4</strong></td><td>A modern Linux distribution is required.</td></tr></tbody></table>

## Disk Size and RAM Requirements

The amount of disk space recommended and RAM you need depends on the [sync mode](fundamentals/sync-modes.md) you want to run. **Current Disk Usage** values listed below are obtained using the standard Erigon + [Caplin](fundamentals/caplin.md)  configuration, with the sole exception of the `--prune.mode` flag.

{% hint style="info" %}
Please note that these figures are from September 2025, and the blockchain data size is continuously increasing.
{% endhint %}

{% tabs %}
{% tab title="Ethereum mainnet" %}
<table data-header-hidden><thead><tr><th width="109"></th><th width="157.11114501953125"></th><th></th><th width="116"></th><th></th></tr></thead><tbody><tr><td><strong>Sync Mode</strong></td><td><strong>Current Disk Usage</strong></td><td><strong>Disk Size (Recommended)</strong></td><td><strong>RAM (Required)</strong></td><td><strong>RAM (Recommended)</strong></td></tr><tr><td>Archive </td><td>1.77 TB</td><td>4 TB</td><td>32 GB</td><td>64 GB</td></tr><tr><td>Full (Default)</td><td>920 GB</td><td>2 TB</td><td>16 GB</td><td>32 GB</td></tr><tr><td>Minimal</td><td>350 GB</td><td>1 TB</td><td>16 GB</td><td>64 GB</td></tr></tbody></table>
{% endtab %}

{% tab title="Gnosis Chain" %}
| **Sync Mode**  | **Current Disk Usage** | **Disk Size (Recommended)** | **RAM (Required)** | **RAM (Recommended)** |
| -------------- | ---------------------- | --------------------------- | ------------------ | --------------------- |
| Archive        | 539 GB                 | 1 TB                        | 16 GB              | 32 GB                 |
| Full (Default) | 462 GB                 | 1 TB                        | 8 GB               | 16 GB                 |
| Minimal        | 128 GB                 | 500 GB                      | 8 GB               | 16 GB                 |
{% endtab %}

{% tab title="Polygon" %}
{% hint style="warning" %}
The final release series of Erigon that officially supports Polygon is 3.1.\*. For the software supported by Polygon, please refer to the link: [https://github.com/0xPolygon/erigon/releases](https://github.com/0xPolygon/erigon/releases).
{% endhint %}

| **Sync Mode**  | **Current Disk Usage** | **Disk Size (Recommended)** | **RAM (Required)** | **RAM (Recommended)** |
| -------------- | ---------------------- | --------------------------- | ------------------ | --------------------- |
| Archive        | 4.85 TB                | 8 TB                        | 64 GB              | 128 GB                |
| Full (Default) | 3.3 TB                 | 4 TB                        | 32 GB              | 64 GB                 |
| Minimal        | 1.2 TB                 | 2 TB                        | 32 GB              | 64 GB                 |
{% endtab %}
{% endtabs %}

{% hint style="success" %}
See also how you can [optimize storage](fundamentals/optimizing-storage.md).
{% endhint %}

## Bandwidth Requirements

Your internet bandwidth is also an important factor, particularly for sync speed and validator performance.

| Node Type      | Bandwidth (Required) | Bandwidth (Recommended) |
| -------------- | -------------------- | ----------------------- |
| Staking/Mining | 10 Mbps              | 50 Mbps                 |
| Non-Staking    | 5 Mbps               | 25 Mbps                 |
