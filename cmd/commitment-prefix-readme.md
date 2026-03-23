## Commitment File visualizer

This tool generates single HTML file with overview of the commitment file.

### Usage

```bash
go build -o comvis ./main.go # build the tool
./comvis <path to commitment file> 
```

```
Usage of ./comvis:

-compression string
    compression type (none, k, v, kv) (default "none")
-j int
    amount of concurrently processed files (default 4)
-output string
    existing directory to store output images. By default, same as commitment files
-trie string
    commitment trie variant (values are hex and bin) (default "hex")
```



