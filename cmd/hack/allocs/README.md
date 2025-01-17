# JSON Transformer

This script transforms an input JSON file into a specific output format.

## Usage

```sh
go run ./cmd/hacks/allocs <inputfile.json>
```


The output will be written to allocs.json.

Note
- The input JSON file should follow the structure defined by the Wrapper and Input types in the script.


### Input JSON Structure

```json
{
  "root": "string",
  "genesis": [
    {
      "contractName": "string",
      "accountName": "string",
      "balance": "string",
      "nonce": "string",
      "address": "string",
      "bytecode": "string",
      "storage": {
        "key1": "value1",
        "key2": "value2"
      }
    }
  ]
}
```
