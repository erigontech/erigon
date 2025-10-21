---
description: Seeding/downloading historical data
---

# Downloader

The Downloader is a service responsible for seeding and downloading historical data using the BitTorrent protocol. Data is stored in the form of immutable `.seg` files, known as **snapshots**. The Ethereum core instructs the Downloader to download specific files, identified by their unique info hashes, which include both block headers and block bodies. The Downloader then communicates with the BitTorrent network to retrieve the necessary files, as specified by the Ethereum core.

{% hint style="warning" %}
**Info**: While all Erigon components are separable and can be run on different machines, the Downloader must run on the same machine as Erigon to be able to share downloaded and seeded files.
{% endhint %}

For a comprehensive understanding of the Downloader's functionality, configuration, and usage, please refer to [./cmd/downloader/README.md](https://github.com/erigontech/erigon/blob/main/cmd/downloader/readme.md) with the following key topics:

1. **Snapshots overview**: An introduction to snapshots, their benefits, and how they are created and used in Erigon.
2. **Starting Erigon with snapshots support**: Instructions on how to start Erigon with snapshots support, either by default or as a separate process.
3. **Creating new networks or bootnodes**: A guide on how to create new networks or bootnodes, including creating new snapshots and starting the Downloader.
4. **Architecture**: An overview of the Downloader's architecture, including how it works with Erigon and the different ways .torrent files can be created.
5. **Utilities**: A list of available utilities, including `torrent_cat`, `torrent_magnet`, and `torrent_clean`.
6. **Remote manifest verify**: Instructions on how to verify that remote webseeds have available manifests and all manifested files are available.
7. **Faster rsync**: Tips on how to use `rsync` for faster file transfer.
8. **Release details**: Information on how to start automatic commits of new hashes to the `master` branch.
9. **Creating a seedbox**: A guide on how to create a seedbox to support a new network or type of snapshots.

Some of the key sections in the documentation include:

* **How to create new snapshots**: Instructions on how to create new snapshots, including using the `seg` command and creating .torrent files.
* **How to start the Downloader**: Instructions on how to start the Downloader, either as a separate process or as part of Erigon.
* **How to verify .seg files**: Instructions on how to verify that .seg files have the same checksum as the current .torrent files.

By referring to the embedded documentation file, you can gain a deeper understanding of the Downloader's capabilities and how to effectively utilize it in your Erigon setup.

## Command line options

To display available options for downloader digit:

```bash
./build/bin/downloader --help
```

The `--help` flag listing is reproduced below for your convenience.

```
snapshot downloader

Usage:
   [flags]
   [command]

Examples:
go run ./cmd/downloader --datadir <your_datadir> --downloader.api.addr 127.0.0.1:9093

Available Commands:
  completion      Generate the autocompletion script for the specified shell
  help            Help about any command
  manifest        
  manifest-verify 
  torrent_cat     
  torrent_clean   Remove all .torrent files from datadir directory
  torrent_create  
  torrent_hashes  
  torrent_magnet  

Flags:
      --chain string                       name of the network to join (default "mainnet")
      --datadir string                     Data directory for the databases (default "/home/admin/.local/share/erigon")
      --db.writemap                        Enable WRITE_MAP feature for fast database writes and fast commit times (default true)
      --diagnostics.disabled               Disable diagnostics
      --diagnostics.endpoint.addr string   Diagnostics HTTP server listening interface (default "127.0.0.1")
      --diagnostics.endpoint.port uint     Diagnostics HTTP server listening port (default 6062)
      --diagnostics.speedtest              Enable speed test
      --downloader.api.addr string         external downloader api network address, for example: 127.0.0.1:9093 serves remote downloader interface (default "127.0.0.1:9093")
      --downloader.disable.ipv4            Turns off ipv6 for the downloader
      --downloader.disable.ipv6            Turns off ipv6 for the downloader
  -h, --help                               help for this command
      --log.console.json                   Format console logs with JSON
      --log.console.verbosity string       Set the log level for console logs (default "info")
      --log.delays                         Enable block delay logging
      --log.dir.disable                    disable disk logging
      --log.dir.json                       Format file logs with JSON
      --log.dir.path string                Path to store user and error logs to disk
      --log.dir.prefix string              The file name prefix for logs stored to disk
      --log.dir.verbosity string           Set the log verbosity for logs stored to disk (default "info")
      --log.json                           Format console logs with JSON
      --metrics                            Enable metrics collection and reporting
      --metrics.addr string                Enable stand-alone metrics HTTP server listening interface (default "127.0.0.1")
      --metrics.port int                   Metrics HTTP server listening port (default 6061)
      --nat string                         NAT port mapping mechanism (any|none|upnp|pmp|stun|extip:<IP>)
                                           			 "" or "none"         Default - do not nat
                                           			 "extip:77.12.33.4"   Will assume the local machine is reachable on the given IP
                                           			 "any"                Uses the first auto-detected mechanism
                                           			 "upnp"               Uses the Universal Plug and Play protocol
                                           			 "pmp"                Uses NAT-PMP with an auto-detected gateway address
                                           			 "pmp:192.168.0.1"    Uses NAT-PMP with the given gateway address
                                           			 "stun"               Uses STUN to detect an external IP using a default server
                                           			 "stun:<server>"      Uses STUN to detect an external IP using the given server (host:port)
                                           
      --pprof                              Enable the pprof HTTP server
      --pprof.addr string                  pprof HTTP server listening interface (default "127.0.0.1")
      --pprof.cpuprofile string            Write CPU profile to the given file
      --pprof.port int                     pprof HTTP server listening port (default 6060)
      --seedbox                            Turns downloader into independent (doesn't need Erigon) software which discover/download/seed new files - useful for Erigon network, and can work on very cheap hardware. It will: 1) download .torrent from webseed 2) download new files after upgrade 3) we planing add discovery of new files soon
      --torrent.conns.perfile int          Number of connections per file (default 10)
      --torrent.download.rate string       Bytes per second, example: 32mb (default "128mb")
      --torrent.download.slots int         Amount of files to download in parallel. (default 128)
      --torrent.maxpeers int               Unused parameter (reserved for future use) (default 100)
      --torrent.port int                   Port to listen and serve BitTorrent protocol (default 42069)
      --torrent.staticpeers string         Comma separated host:port to connect to
      --torrent.upload.rate string         Bytes per second, example: 32mb (default "4mb")
      --torrent.verbosity int              0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail (must set --verbosity to equal or higher level and has default: 2) (default 2)
      --trace string                       Write execution trace to the given file
      --verbosity string                   Set the log level for console logs (default "info")
      --verify                             Verify snapshots on startup. It will not report problems found, but re-download broken pieces.
      --verify.failfast                    Stop on first found error. Report it and exit
      --verify.files string                Limit list of files to verify
      --webseed string                     Comma-separated URL's, holding metadata about network-support infrastructure (like S3 buckets with snapshots, bootnodes, etc...)

Use " [command] --help" for more information about a command.
```
