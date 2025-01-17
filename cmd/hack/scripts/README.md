# Scripts Directory

This directory is intended to host all future scripts related to the project. Below is a brief description of the purpose and usage of this directory.

## Scripts

<!-- list for faster access in the future I guess ? -->
link to scripts

- [Geth Log parser](#geth-log-parser)

### Geth Log Parser

This script parses Geth logs to extract block sync, memory usage, database size, and trie nodes data, outputting it to a CSV file. Use `python3` to run the script.

```sh
python3 cmd/hack/scripts/geth_log_parse.py /path/to/geth.log
```
