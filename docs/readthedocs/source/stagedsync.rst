==========
StagedSync
==========
We tried to maximally decompose the sync process into stages, and perform them sequentially. From the first sight, it might sound like a bad idea - why not use concurrency? However, we noticed that running many things concurrently obscured our ability to profile and optimise code - there is simply too much going on. Decomposition allowed us to optimise each stage in separation, which is much more tractable. We also noticed the benefit of improved code structure.

All of the stages are declared in https://github.com/erigontech/erigon/blob/master/eth/stagedsync/stagebuilder.go

Stage 1 : Download Block Headers
================================

.. code-block:: go

    	{
			ID: stages.Headers,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.Headers,
					Description: "Download headers",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnHeaderDownloadStage(s, u, world.d, world.headersFetchers)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return u.Done(world.db)
					},
				}
			},
		},

This stage uses two processes, a fetcher method and a processor method.

.. code-block:: go

    func (d *Downloader) fetchHeaders(p *peerConnection, from uint64) error

    func (d *Downloader) processHeaders(origin uint64, pivot uint64, blockNumber uint64) error

the fetcher method retrieve from the peer a group of block headers encoded in RLP, decode them and send them to the process method.

the process method takes the headers retrieve thanks to the fetcher and does the following:

    * Extract Difficulty from each block in the database and record total Difficulty in the database.

    * Save block headers in database.

This process repeats until we reach the maximum height. once it is reached the stage finish.

Changes in DB:

    * Headers are encoded in database under bucket `dbutils.HeaderPrefix`

    * Total Difficulty per block is written in `dbutils.HeaderTDKey`

Stage 2 : Write Block Hashes
============================

.. code-block:: go

		{
			ID: stages.BlockHashes,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.BlockHashes,
					Description: "Write block hashes",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnBlockHashStage(s, world.db, world.datadir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return u.Done(world.db)
					},
				}
			},
		},

This stage takes the data written in stage 1, and by using the ETL framework, it writes (blockHash => blockNumber) in order in the Database. So that with given block hash we can get given block number and thus retrieve a block by knowing only the hash and not the number. We use a different stage because we want to write pre-sorted data in order to minimize write operations in the Database.

.. code-block:: go

    func extractHeaders(k []byte, v []byte, next etl.ExtractNextFunc) error

The function above is used with ETL to extract blockHashes and blockNumber from the key of the headers bucket. In fact, since the key of the headers bucket are a concatenation of blockNumber and blockHash, they can be used in such process.

Changes in DB:

    * BlockHash => BlockNumber are written in bucket `dbutils.HeaderNumberPrefix`

Stage 3 : Download Block Bodies
===============================

.. code-block:: go

		{
			ID: stages.Bodies,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.Bodies,
					Description: "Download block bodies",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return spawnBodyDownloadStage(s, u, world.d, world.pid, world.prefetchedBlocks)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return unwindBodyDownloadStage(u, world.db)
					},
				}
			},
		},

This stage, downloads block bodies and put them into the database. This stage is divided into two processes:

.. code-block:: go

	func (d *Downloader) fetchBodies(from uint64) error

	func (d *Downloader) processBodiesStage(logPrefix string, to uint64) error

`fetchBodies` downloads the bodies from the peer and decode them from RLP format.

`processBodiesStage` takes the bodies downloaded and those the following with them:

	* Verify them.
	* RLP-encode them.
	* compress the rlp-encoded bodies using `snappy`.
	* put the compressed RLP into the database.

in order for Erigon to reaccess the block bodies, it decompresses them and rlp-decode them. the entries in the db for block bodies are a concatenation of [block number] + [block hash] in order to pre-sort them before inserting them into the database.

Changes in DB:

    * [block number] + [block hash] => Block bodies are written in bucket `dbutils.BlockBodyPrefix`
