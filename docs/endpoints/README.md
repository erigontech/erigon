# JSON RPC Endpoints

This tool is used to generate the list of supported endpoints provided by the JSON-RPC server as a markdown document.

It uses reflection to go through all API interfaces and then merge the information with the content of the [template.md](./template.md) as the base to generate the [endpoints.md](./endpoints.md) file.

To generate the file ensure you have `make` and `go` installed, then run available on this directory:

```bash
make gen-doc
```

The [endpoints.md](./endpoints.md) must always be generated when a new endpoint is added, removed or changed so we can have this file pushed to the repo for further use.

There is also a command to check if the current file is compatible with the current code. This command is meant to be used in the CI do ensure the doc is updated.

```bash
make check-doc
```