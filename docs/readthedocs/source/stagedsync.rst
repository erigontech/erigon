==========
StagedSync
==========
We tried to maximally decompose the sync process into stages, and perform them sequentially. From the first sight, it might sound like a bad idea - why not use concurrency? However, we noticed that running many things concurrently obscured our ability to profile and optimise code - there is simply too much going on. Decomposition allowed us to optimise each stage in separation, which is much more tractable. We also noticed the benefit of improved code structure.

All of the stages are declared in https://github.com/ledgerwatch/turbo-geth/blob/master/eth/stagedsync/stagebuilder.go

Stage1 : Download Block Headers
===============================

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

This stage takes advantage of two processes, a fetcher method and a processer method.

.. code-block:: go

    func (d *Downloader) fetchHeaders(p *peerConnection, from uint64) error

    func (d *Downloader) processHeaders(origin uint64, pivot uint64, blockNumber uint64) error

the fetcher method retrieve from the peer a group of block headers encoded in RLP, decode them and send them to the process method.

the process method takes the headers retrieve thanks to the fetcher and does the following:

    * Extract Difficulty from each block in the database and record total Difficulty in the database.

    * Save block headers in database.

This process repeates until we reach the maximun height. once it is reached the stage finish.

Changes in DB:
    * Headers are encoded in database under bucket `dbutils.HeaderPrefix`

    * Total Difficulty per block is written in `dbutils.HeaderTDKey`