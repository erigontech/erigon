package torrent

/*
	{
		ID:          stages.DownloadHeadersSnapshot,
		Description: "Download headers snapshot",
		ExecFunc: func(s *StageState, u Unwinder) error {
			return SpawnHeadersSnapshotDownload(s,stateDB,datadir, quitCh )
		},
		UnwindFunc: func(u *UnwindState, s *StageState) error {
			return u.Done(stateDB)
		},
		Disabled: !snapshotMode.Headers,
		DisabledDescription: "Experimental stage",
	},


		{
			ID:          stages.DownloadBodiesSnapshot,
			Description: "Download bodies snapshot",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(stateDB)
			},
			Disabled: !snapshotMode.Bodies,
			DisabledDescription: "Experimental stage",
		},

		{
			ID:          stages.DownloadStateStateSnapshot,
			Description: "Download state snapshot",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(stateDB)
			},
			Disabled: !snapshotMode.State,
		},
		{
			ID:          stages.DownloadReceiptsSnapshot,
			Description: "Download receipts snapshot",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(stateDB)
			},
			Disabled: !snapshotMode.Receipts,
		},

 */