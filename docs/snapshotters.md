
# Snapshotters

These are servers which publish file. Each server is running one chain. Look for "Erigon Machines" docs and search for "snapshotter" prefix.

What to write about?
- how to setup such a machine?
- what is the process? - TODO: provisioning of a server (by Olex) and getting creds,
- snapshots automation GH action ([code](https://github.com/erigontech/erigon-snapshots-automation/blob/main/.github/workflows/snapshots_automation.yml) and [SnapRelease workflow action](https://github.com/erigontech/erigon-snapshots-automation/actions/workflows/snapshots_automation.yml)).
- this script copies over necessary creds to the snapshotter and triggers the [snaprelease-remote.sh](https://github.com/erigontech/scripts/blob/main/snapshots/snaprelease/snaprelease-remote.sh#L1189) on that machine. Therefore,
- When a new release is added - e.g. we're deprecating 3.0 and adding 3.2, the snapshotter name might also be changed. Then we update these available version in yml file -- this will effect the release options available for `SnapRelease`.
- also, when a new chain is added e.g. "chain GigaChad", and we provision a server `snapshotter-v35-gigachad-n30`, we need to update this script as well.

# Table of contents
- [Snapshot automation process](#snapshot-automation-process)
- [SnapRelease workflow](#snaprelease-workflow)
  - [force updated hashes](#force-updated-hashes)
  - [Notification on errors](#notification-on-errors)
- [Tooling for fixing broken snapshotters](#tooling-for-fixing-broken-snapshotters)
  - [killing and restarting erigon](#killing-and-restarting-erigon)
  - [Inspection and checks](#inspection-and-checks)
    - [integrity checks and publishable](#integrity-checks-and-publishable)
    - [inspecting state progress](#inspecting-state-progress)
    - [block num <> txnum conversion](#block-num--txnum-conversion)
  - [how to generate/regenerate data](#how-to-generateregenerate-data)
    - [how to generate receipt/rcache and "standalone inverted indexes"](#how-to-generate-receiptrcache-and-standalone-inverted-indexes)
      - [scenario1](#scenario1)
      - [scenario2](#scenario2)
    - [how to regenerate state files](#how-to-regenerate-state-files)
    - [restore missing files from bucket](#restore-missing-files-from-bucket)
    - [note for when you're regenerating snapshots](#note-for-when-youre-regenerating-snapshots)
    - [run integrity check after regen/restore is done](#run-integrity-check-after-regenrestore-is-done)
  - [taking backups](#taking-backups)
- [Adding a new release version to release automation](#adding-a-new-release-version-to-release-automation)
- [Adding new chain to release automation](#adding-new-chain-to-release-automation)
- [new snapshotter for existing chain](#new-snapshotter-for-existing-chain)
- [Setup steps for new snapshotter](#setup-steps-for-new-snapshotter)
  - [Other tools](#other-tools)


## Snapshot automation process

- snapshotters run the erigon node and produce snapshots. We run this for each chain we want to publish snapshots for.
- The snapshot automation process broadly must:
	- account for the snapshots the snapshotters have
	- validate those snapshots, raising alarm if something goes wrong
	- as much as possible, automate the process of any recovery to make the snapshots publishable (e.g. delete overlapping files)
	- publish the snapshots to [R2 bucket](https://github.com/erigontech/scripts/blob/main/snapshots/snaprelease/snaprelease-remote.sh#L246), and publish the toml file (containing torrent hashes of those files) to `erigontech/erigon-snapshot` repo and R2 bucket. The erigon nodes can then access these files via [webseeds](https://github.com/erigontech/erigon-snapshot/blob/main/webseed/mainnet.toml)
	- perform any CDN cache purging or expire old files in buckets (we have 8w expiry currently).
	- restart the erigon node on snapshotter, so that it continues producing new snapshots to be release.

snapshots release process is run by devops every week, typically around start of the week.
Next we look at the workflow they trigger for this...
## SnapRelease workflow

- [SnapRelease workflow action](https://github.com/erigontech/erigon-snapshots-automation/actions/workflows/snapshots_automation.yml) triggers `snaprelease-remote.sh` on the snapshotter machine. Choose the relevant options in workflow carefully. 
- when the action is triggered, it must be approved. Currently the approvers are Alex and Oleksandr.
- Once approved, the `snaprelease-remote.sh` is copied and executed on the snapshotter. It includes the following steps for snapshot release:
	- check integrity
	- generate the torrent files (if missing) + torrent file verification
	- "smart copy" files to R2 bucket (data from these buckets are served via [webseeds](https://github.com/erigontech/erigon-snapshot/tree/main/webseed) to the client).
	- purge the CDN cache (which sits infront of the bucket) if enabled.
	- raise PR to `erigontech/erigon-snapshot` repo to update the toml file of the chain.
	- once the above is approved and merged, [mirror workflow](https://github.com/erigontech/erigon-snapshot/actions/workflows/mirror.yml) updates the same toml to R2 bucket as well.

### force updated hashes

When triggering the workflow, one option presented is **Should I FORCE publishing DESPITE hash changes?**

- a snapshot might be "rewritten" due to different reasons e.g. maybe it was regenerated (but the version was not updated). So the contents available for the same file in bucket and in snapshotter will be different. In this case the triggerer should decide if he wants to force publish (despite the contents changing) or if that was unexpected and there should be an error.
- Also look at "note for when you're regenerating snapshots" section for relevant .torrent commands.

### Notification on errors

if there's an error in the "release process", it is floated upto discord or email.

- the receiver list (for both discord and email) is in `snaprelease-remote.sh`
- "unchanged hashes" and `--force` option
- "clear indexes" option
  

SETUP:
- what are the steps to setup a new snapshotter for a new chain or release (assume not recycling the same server)?
- some repo from which erigon start flags were been downloaded (and what flags must be there)?

  

MANUAL interventions on the snapshotters:

- get pem from Olex to get into the server
- maybe there was a integrity check/publishable failure. Know more about those [here](##)
- maybe there was an integrity error, and you need to regen the data/files: These are discussed in 
- then after regen how to do integrity check again?
- how to restore "previous file" (from R2 bucket) if current one is corrupted...e.g. sometimes torrent verify might fail, how to recover from those...
- spawn_custom_trace command...
- how to create backup of whole datadir (or partial one)
- regen indices or seg retire
- how to remove latest state snapshot files (rm-state)
- compact_domains optimization

## Tooling for fixing broken snapshotters

### killing and restarting erigon

**killing**
- the snapshotter will have erigon running, you might need to kill that as most debugging/"recovery" commands flock on the datadir
- typically, use SIGINT (`kill -2`) and if it doesn't kill erigon within few seconds (there might be long background processes like files build or merge that prevent immediate shutdown), you can use SIGKILL (`kill -9`)
- even directly using SIGKILL is not bad. Erigon is supposed to be safe from direct shutdowns or crashes. If erigon ends up in a bad state after SIGKILL, it's a bug and we need to fix & add protection against it.

**restarting**
- just grep history for last erigon command (with nohup and output redirection) and restart with that once done with repairing snapshotter. 

### Inspection and checks

#### integrity checks and publishable

- integrity checks might be one thing due to which snapshotters is broken.

```bash
go run ./cmd/erigon seg integrity --datadir /erigon-data
```

- publishable

```bash
go run ./cmd/erigon seg publishable ls --datadir /erigon-data
```

publishable checks not "contents of file" but continuity i.e. there are no gaps between files, it starts from 0, there are no overlapping files. The check involves inspecting filenames rather than file contents.

#### inspecting state progress

use `integration print_stages`

```bash
go run ./cmd/integration print_stages --datadir ../datadir/ethmainnet_full


 			     stage_at 	 prune_at
OtterSync 		 23589849 	 0
Headers 		 23589850 	 0
BlockHashes 	 23589850 	 0
Bodies 			 23589850 	 0
Senders 		 23589850 	 0
Execution 		 23589850 	 23589850
CustomTrace 		 0 		 0
Translation 		 0 		 0
TxLookup 		 23589850 	 23515999
Finish 			 23589850 	 0
--
prune distance: full

blocks: segments=23587999, indices=23587999
blocks.bor: segments=0, indices=0
state.history: idx steps: 2.58, TxNums_Index(23589850,3090064301)

sequence: EthTx=3090065144

in db: first header 23584407, last header 23589850, first body 23584407, last body 23589850
--


domain and ii progress

Note: progress for commitment domain (in terms of txNum) is not presented.

 			     historyStartFrom 		 progress(txnum) 		 progress(step)
accounts 		 3000000000 			 3090064301 			 1977
storage 		 3000000000 			 3090064299 			 1977
code 			 3000000000 			 3090064295 			 1977
commitment 		 - 				         - 				         1977
receipt 		 3000000000 			 3090064301 			 1977
rcache 			 1800000000 			 3090064301 			 1977

logtopics 		 - 				         3090064300 			 1977
logaddrs 		 - 				         3090064300 			 1977
tracesfrom 		 - 				         3090064300 			 1977
tracesto 		 - 				         3090064301 			 1977
--
```

#### block num <> txnum conversion

```

# block num -> txnum
go run ./cmd/erigon seg txnum --datadir ../datadir/ethmainnet_main_aug --block 23233761

# txnum -> block num
go run ./cmd/erigon seg txnum --datadir ../datadir/ethmainnet_main_aug --txnum 292187500

```

### how to generate/regenerate data

erigon has various ways to generate/regenerate data (in db as well as files). 
Following things are discussed:

1. `stage_custom_trace`: for any of tracesto, tracesfrom, logaddr, logtopics, receipt, rcache
2. `stage_exec`: for other data (blocks/txs etc.)
3. restore missing files from bucket
4. regen .torrent after regen
5. run integrity check after regen/restore is done


#### how to generate receipt/rcache and "standalone inverted indexes"

standalone inverted indexes: tracesto, tracesfrom, logaddr, logtopics...

- if you need to generate/regenerate data for these 6 types, `stage_custom_trace` is your friend. I'll assume `receipt` domain has to be generated in following examples. Also not specifying `--datadir` and `--chain` flags.
- first you need to decide the point `from` which the state has to be generated. This depends on the current progress. Use `print_stages` to find progress.
- All data since `from` should then be deleted. Note that this deletion process (using `rm chaindata` or `stage_custom_trace --reset`) can delete much more data. This is okay. Regeneration will fill it up. 

##### scenario1

`from` resides in db i.e. you're happy with the data in snapshot files, but want to re-generate the data in db.

**reset the data in db**

```bash
./build/bin/integration stage_custom_trace --domain=receipt --reset
```

**regen the data** 

```bash
./build/bin/integration stage_custom_trace --domain=receipt
```


##### scenario2

`from` resides in files 
1. reset data in db (`stage_custom_trace --reset`)
2. delete receipt files 
```bash
./build/bin/erigon seg rm-state-snapshots --domain=receipt --latest
```

delete all files of receipts (will cause regeneration right from first tx)
```bash
./build/bin/erigon seg rm-state-snapshots --domain=receipt
```

3. regen data (`stage_custom_trace`)

When done, see if  [run integrity check after regen/restore is done](#run-integrity-check-after-regenrestore-is-done) section is needed.

#### how to regenerate state files

for accounts/storage/code etc., you must use stage_exec...
1. reset stage_exec `integration stage_exec --reset`: this will clear all state data (and other auxiliary tables) from the db
2. if necessary, use `erigon seg rm-state-snapshots` to delete files (latest or all)
3. then `integration stage_exec` to re-execute and generate state data

When done, see if  [run integrity check after regen/restore is done](#run-integrity-check-after-regenrestore-is-done) section is needed.


#### restore missing files from bucket

- last releases we came across issues where there were gaps in blocks snapshots. Due to some bug files got deleted during merge.
- regeneration is an option for state files, but not for block files. Also regen might be expensive
- another way to deal with it: simply download the file from bucket, and restore from there.

```bash
aria2c -j4 -x4 -s4 -c --auto-file-renaming=false --dir=./ --continue=true --console-log-level=warn --summary-interval=0 --max-tries=10 --retry-wait=5 --header="<ASK-DEVOPS>" https://erigon31-v1-snapshots-mainnet.erigon.network/history/v1.0-accounts.1888-1890.v
```

When done, see if  [run integrity check after regen/restore is done](#run-integrity-check-after-regenrestore-is-done) section is needed

#### note for when you're regenerating snapshots

- so when you regen existing snapshots on a snapshotter, their .torrent becomes obsolete. Ensure to regen the torrent files as well. 
- e.g. for specific file
```bash
rm <datadir>/history/v1.0-accounts.3552-3554.v.torrent
go run ./cmd/downloader torrent_create --datadir <datadir>  --file v1.0-accounts.3552-3554.v --chain mainnet
```
- if there are multiple such gen files, use `--all` flag:
```bash
# rm those .torrent files
go run ./cmd/downloader torrent_create --datadir <datadir>  --all --chain mainnet
```
- if you don't know exactly which files were regenerated, a simpler route might be to simply remove all .torrent files using `downloader torrent_clean` and then `downloader torrent_create --all`

- if this step is skipped, the snapshot release process will fail because it does .torrent verification with `downloader --verify --verify.failfast --datadir <datadir>`

#### run integrity check after regen/restore is done

Once recovery is done:

- Don't forget to <u>run integrity checks again</u>. If it passes, data is probably fine.
- currently publishable check is part of integrity check. After recovery, errors like "overlapping files" is okay - it just means that erigon hasn't merged those files. You can run `seg retire` to force the merge and remove the overlaps. Integrity check should work then.
	- note that snapshot automation does the same thing - `seg retire` before running integrity checks. This gives the check maximal chance to succeed.

### taking backups

1. sometimes you might want to take backup of file(s) and then regen it. If there's few files you'll be touching and you don't need entire backup, it might be useful to just copy over to a separate folder. 

2. taking backup of all snapshots and mdbx:
`./cmd/scripts/mirror-datadir.sh <source> <destination>`  can be used to do fast cheap backups. A sample use would be 
```bash
./cmd/scripts/mirror-datadir.sh /erigon-data /erigon-data/backup
```

 the script creates hardsymlinks of the snapshot files, while it copies editable files like mdbx.dat. Then you can do your thing on original datadir, and use the backup if needed.

3. devops also occasionally take backups of the snapshots. Consult them if that's your requirement and need one.

## Adding a new release version to release automation

so we need to add the new release (and maybe take out the old release) in few places:
- [SnapRelease gh script](https://github.com/erigontech/erigon-snapshots-automation/blob/main/.github/workflows/snapshots_automation.yml)
	- `erigon_release` input
	- maybe other logic relying on precise release version
- add/remove server from [ansible inventory](https://github.com/erigontech/scripts/blob/main/snapshots/snaprelease/snapshotters.yml)
- [snaprelease-remote.sh](https://github.com/erigontech/scripts/blob/main/snapshots/snaprelease/snaprelease-remote.sh) has some hardcoded logic using the exact release versions (hardcoded public buckets, [v31 vs v32](https://github.com/erigontech/scripts/blob/main/snapshots/snaprelease/snaprelease-remote.sh#L239) etc.) -- those also need to be changed.

## Adding new chain to release automation

### setup snapshotter

- need to add entry in [erigon-launch-params for snapshotter](https://github.com/erigontech/erigon-launch-params/tree/main/snapshotters/v31). This will be passed to `--config` when launching erigon in the snapshotter.
- clone `erigon-launch-param` and `erigon` repo in HOME folder.
- ideally we want to reduce number of moving pieces in the erigon instance of snapshotter. So consider adding things like `txpool.disable` for it. Look at other chains yml for what more should go
- `--persist.receipts` should also be added, since it's disabled by default for archive nodes.
- you can then launch erigon. We've typically `nohup` and redirect stdout and stderr to `/erigon-logs/erigon-process.log`. e.g.
```bash
nohup /home/ubuntu/erigon/build/bin/erigon --config /home/ubuntu/erigon-launch-params/snapshotters/v31/ethmainnet/ethmainnet-config.yml --persist.receipts  --torrent.upload.rate=200m  > /erigon-logs/erigon-process.log 2>&1 &
```


### for release automation

- add (touch and commit to main) - `$CHAIN.toml` in `erigontech/erigon-snapshot` repo 
- [SnapRelease gh script](https://github.com/erigontech/erigon-snapshots-automation/blob/main/.github/workflows/snapshots_automation.yml)
- add/remove server from [ansible inventory](https://github.com/erigontech/scripts/blob/main/snapshots/snaprelease/snapshotters.yml)
- [snaprelease-remote.sh](https://github.com/erigontech/scripts/blob/main/snapshots/snaprelease/snaprelease-remote.sh)
	- not much chain specific logic, but contains the "bucket name template" or reviewers list for PRs on erigon-snapshot repo. Review those.


arb-sepolia has a complicated setup, and the much logic had to be added to the snaprelease-remote.sh.
Other chains which have setups closer to ethmainnet should work fine without much change in the bash script.


## new snapshotter for existing chain

- `Adding new chain to release automation -> for release automation` should already be done.
- do "Adding new chain to release automation -> setup snapshotter"
- first you launch the snapshotter with downloader; then once synced, subsequent runs are done without the downloader (i.e. `--no-downloader` flag). See [this](https://github.com/erigontech/erigon-launch-params/blob/main/snapshotters/v31/ethmainnet/LAUNCH.md)



## Setup steps for new snapshotter

  

"INFO" stuff:
- talk a bit about integrity checks, where to find it, purpose, encourage to think and add integrity check.
- link to Michele's presentations
- mention about torrent_hashes/torrent_create/`seg retire`

  
  

---

outline:

  

- new chain has come, what changes in related scripts can help setup a snapshotter and automated release off that server?

- other less involved stuff: existing snapshotter recycled for new release - what steps needed then? Or new chain and it's snapshottter is ready, what steps needed then?

- what kind of errors float up when snap release is triggered? Document tools that can be used to regen files, check integrity, safely manipulate files and db, debug etc.

- point to appropriate presentations/documentations/code...


---

## Other tools

how chain toml is produced
`./build/bin/downloader torrent_hashes --datadir <datadir> --chain $CHAIN >> chain.toml`