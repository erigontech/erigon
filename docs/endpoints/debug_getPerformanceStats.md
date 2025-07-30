# debug_getPerformanceStats

Returns performance statistics about the node including TPS (transactions per second) and MGas per second metrics.

## Parameters

- `startBlock` (optional): The starting block number for the range. If not provided, starts from genesis block (0).
- `endBlock` (optional): The ending block number for the range. If not provided, ends at the latest block.

Block number parameters can be:
- A specific block number (e.g., `1000000`)
- `"latest"` - the latest block
- `"earliest"` - the genesis block (0)

## Returns

```json
{
  "maxTPS": 150.5,
  "maxMGasPerSecond": 45.2,
  "averageTPS": 120.3,
  "averageMGasPerSecond": 35.8,
  "medianTPS": 118.7,
  "medianMGasPerSecond": 34.2,
  "blockRange": {
    "start": 1000000,
    "end": 1001000
  },
  "totalBlocks": 1001,
  "totalTransactions": 120500,
  "totalGasUsed": 35800000000
}
```

### Field Descriptions

- `maxTPS`: Maximum transactions per second observed in the block range
- `maxMGasPerSecond`: Maximum MGas per second observed in the block range
- `averageTPS`: Average transactions per second (only included if block range provided)
- `averageMGasPerSecond`: Average MGas per second (only included if block range provided)
- `medianTPS`: Median transactions per second (only included if block range provided)
- `medianMGasPerSecond`: Median MGas per second (only included if block range provided)
- `blockRange`: The block range that was analyzed
- `totalBlocks`: Total number of blocks analyzed
- `totalTransactions`: Total number of transactions in the range
- `totalGasUsed`: Total gas used in the range

## Examples

### Get stats for entire chain
```json
{
  "jsonrpc": "2.0",
  "method": "debug_getPerformanceStats",
  "params": [],
  "id": 1
}
```

### Get stats for specific block range
```json
{
  "jsonrpc": "2.0",
  "method": "debug_getPerformanceStats",
  "params": [1000000, 1001000],
  "id": 1
}
```

### Get stats from genesis to latest
```json
{
  "jsonrpc": "2.0",
  "method": "debug_getPerformanceStats",
  "params": ["earliest", "latest"],
  "id": 1
}
```



## Notes

- TPS and MGas/s are calculated between consecutive blocks based on the time difference
- If there are gaps in the blockchain or missing blocks, they are skipped
- The endpoint is useful for analyzing network performance over time
- Large block ranges may take longer to process
- Statistics (average, median) are only calculated when a block range is provided
- **Data Freshness**: The endpoint uses database transactions that provide a consistent snapshot. Results may not include the very latest blocks if the node is actively syncing.
- **Stage Progress**: The endpoint uses `stages.Execution` to determine the latest block, providing more up-to-date results compared to fully finalized blocks. 