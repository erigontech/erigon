# Snapshotters 

These are servers which publish file. Each server is running one chain. Look for "Erigon Machines" docs and search for "snapshotter" prefix.

What to write about?
- how to setup such a machine?
  - what is the process? - TODO: provisioning of a server (by Olex) and getting creds, 
  - snapshots automation GH action ([code](https://github.com/erigontech/erigon-snapshots-automation/blob/main/.github/workflows/snapshots_automation.yml) and [SnapRelease workflow action](https://github.com/erigontech/erigon-snapshots-automation/actions/workflows/snapshots_automation.yml)).
    - this script copies over necessary creds to the snapshotter and triggers the [snaprelease-remote.sh](https://github.com/erigontech/scripts/blob/main/snapshots/snaprelease/snaprelease-remote.sh#L1189) on that machine. Therefore,
    - When a new release is added - e.g. we're deprecating 3.0 and adding 3.2, the snapshotter name might also be changed. Then we update these available version in yml file -- this will effect the release options available for `SnapRelease`.
    - also, when a new chain is added e.g.  "chain GigaChad", and we provision a server `snapshotter-v35-gigachad-n30`, we need to update this script as well. 

  - The snapshot "release process" executed on the snapshotter i.e. logic of `snaprelease-remote.sh` above includes several steps:
    - check integrity
    - generate the torrent files
    - "smart copy" files to R2 bucket (data from these buckets are served via [webseeds](https://github.com/erigontech/erigon-snapshot/tree/main/webseed) to the client).
    - purge the CDN cache (which sits infront of the bucket) if enabled.
    - update the "TOML file"(e.g. [TOML for mainnet](https://github.com/erigontech/erigon-snapshot/blob/main/mainnet.toml)), which needs to happen on both `erigontech/erigon-snapshot` repo AND the R2 bucket.
    - TODO: who are these PRs sent to? Is the process stopped till then? (can't update R2 before PR is approved and merged.)

- if there's an error in the "releaes process", that is floated upto discord or email. 
  - the receiver list is in `snaprelease-remote.sh`
  - "unchanged hashes" and `--force` option
  - "clear indexes" option


SETUP:
- what are the steps to setup a new snapshotter for a new chain or release (assume not recycling the same server)?
- some repo from which erigon start flags were been downloaded (and what flags must be there)?
- INFO: talk a bit about integrity checks, where to find it, purpose, encourage to think and add integrity check.

MANUAL interventions on the snapshotters:
- get pem from Olex to get into the server
- maybe there was an integrity error, and you need to regen: TODO...document various scenarios 
- when regen is done, MUST regen torrent files
- then after regen how to do integrity check again?
- how to restore "previous file" (from R2 bucket) if current one is corrupted...e.g. sometimes torrent verify might fail, how to recover from those...
- spawn_custom_trace command...
- how to create backup of whole datadir (or partial one)
- regen indices or seg retire
- how to remove latest state snapshot files (rm-state)
- compact_domains optimization


